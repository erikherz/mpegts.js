/*
 * Copyright (C) 2025 Vivoh, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Log from '../utils/logger.js';
import {BaseLoader, LoaderStatus, LoaderErrors} from './loader.js';
import {RuntimeException} from '../utils/exception.js';

/**
 * Stream Rotation Handler - manages multiple WebTransport streams and provides continuity
 */
class StreamRotationHandler {
    constructor(options = {}) {
        this.TAG = 'StreamRotationHandler';
        this.onLog = options.onLog || ((msg) => Log.v(this.TAG, msg));
        
        // Buffer for data from multiple streams
        this.streamBuffers = new Map(); // Map of streamId -> buffer array
        this.activeStreamIds = new Set(); // Currently active stream IDs
        this.streamPTSRanges = new Map(); // Map of streamId -> {firstPTS, lastPTS}
        
        // PTS tracking for continuity
        this.globalPTSOffset = 0;
        this.lastGlobalPTS = null;
        
        // Ordered packet buffer (across all streams)
        this.orderedPacketBuffer = [];
        this.outputSequence = 0; // Used to track output order
        
        // Configuration
        this.config = {
            maxBufferSize: options.maxBufferSize || 5 * 1024 * 1024, // 5MB per stream
            minBufferTime: options.minBufferTime || 1000, // Min buffering time in ms
            maxBufferTime: options.maxBufferTime || 5000, // Max buffering time in ms
            ptsDiscontinuityThreshold: options.ptsDiscontinuityThreshold || 900000, // 10 seconds in 90kHz clock
            packetSize: 188,
            flushThreshold: options.flushThreshold || 100 // Number of packets to trigger a flush
        };
        
        // Stream transition tracking
        this.lastStreamTransitionTime = 0;
        this.isInTransition = false;
        this.transitionSequenceCounter = 0;
        
        // Statistics
        this.stats = {
            streamsReceived: 0,
            streamsSwitched: 0,
            ptsJumps: 0,
            packetsProcessed: 0,
            packetsOutputted: 0,
            discontinuities: 0,
            streamOverlaps: 0
        };
    }
    
    /**
     * Register a new stream and prepare to receive data
     * @param {string} streamId - Unique identifier for the stream
     */
    registerStream(streamId) {
        if (!this.streamBuffers.has(streamId)) {
            this.onLog(`Registering new stream: ${streamId}`);
            this.streamBuffers.set(streamId, []);
            this.activeStreamIds.add(streamId);
            this.streamPTSRanges.set(streamId, { firstPTS: null, lastPTS: null });
            this.stats.streamsReceived++;
            
            // Record transition if we already had an active stream
            if (this.activeStreamIds.size > 1) {
                this.lastStreamTransitionTime = Date.now();
                this.isInTransition = true;
                this.stats.streamsSwitched++;
                this.transitionSequenceCounter++;
                this.onLog(`Stream transition detected, now have ${this.activeStreamIds.size} active streams`);
            }
        }
    }
    
    /**
     * Process data from a specific stream
     * @param {string} streamId - Stream identifier 
     * @param {Uint8Array} data - MPEG-TS data chunk
     * @param {Object} packetValidator - The TS packet validator to use
     * @returns {Object|null} - Metadata about processing results
     */
    processStreamData(streamId, data, packetValidator) {
        if (!this.streamBuffers.has(streamId)) {
            this.registerStream(streamId);
        }
        
        // Extract complete TS packets from the chunk
        const packets = this._extractCompletePackets(data, streamId);
        if (!packets || packets.length === 0) {
            return null;
        }
        
        // Process each packet: extract PTS, validate, and enqueue
        const processedPackets = this._processAndSortPackets(packets, streamId, packetValidator);
        
        // Check if we need to flush the buffer
        const shouldFlush = this.orderedPacketBuffer.length >= this.config.flushThreshold;
        
        return {
            streamId,
            packetCount: processedPackets,
            shouldFlush,
            hasTransition: this.isInTransition,
            ptsRange: this.streamPTSRanges.get(streamId)
        };
    }
    
    /**
     * Mark a stream as ended, process any remaining data
     * @param {string} streamId - Stream identifier
     */
    endStream(streamId) {
        if (this.activeStreamIds.has(streamId)) {
            this.onLog(`Stream ended: ${streamId}`);
            this.activeStreamIds.delete(streamId);
            
            // Check if there are no more active streams
            if (this.activeStreamIds.size === 0) {
                this.onLog(`All streams ended, flushing any remaining data`);
                // Force a flush of any remaining data
                return this.flushBuffers();
            }
            
            // If the stream transition period is over, update state
            if (this.isInTransition && Date.now() - this.lastStreamTransitionTime > this.config.maxBufferTime) {
                this.isInTransition = false;
                this.onLog(`Stream transition period ended, normal processing resumed`);
            }
        }
        return null;
    }
    
    /**
     * Flush the ordered packet buffer and return packets for demuxing
     * @returns {Array} - Array of ordered TS packets
     */
    flushBuffers() {
        if (this.orderedPacketBuffer.length === 0) {
            return [];
        }
        
        // Sort the buffer by sequence number to ensure proper order
        this.orderedPacketBuffer.sort((a, b) => a.sequence - b.sequence);
        
        // Extract just the packet data, discard the metadata
        const packets = this.orderedPacketBuffer.map(item => item.packet);
        
        // Update statistics
        this.stats.packetsOutputted += packets.length;
        
        // Clear the buffer
        this.orderedPacketBuffer = [];
        
        this.onLog(`Flushed ${packets.length} packets to decoder`);
        return packets;
    }
    
    /**
     * Extract complete 188-byte MPEG-TS packets from a data chunk
     * @param {Uint8Array} data - Raw data chunk
     * @param {string} streamId - Stream identifier
     * @returns {Array} - Array of complete TS packets
     */
    _extractCompletePackets(data, streamId) {
        if (!data || data.length === 0) {
            return [];
        }
        
        // Get the existing buffer for this stream
        const streamBuffer = this.streamBuffers.get(streamId) || [];
        
        // Append new data
        let combinedBuffer;
        if (streamBuffer.length > 0) {
            combinedBuffer = new Uint8Array(streamBuffer.length + data.length);
            combinedBuffer.set(streamBuffer, 0);
            combinedBuffer.set(data, streamBuffer.length);
        } else {
            combinedBuffer = data;
        }
        
        // Find the first sync byte (0x47)
        let syncIndex = -1;
        for (let i = 0; i < combinedBuffer.length; i++) {
            if (combinedBuffer[i] === 0x47) {
                // Verify we have multiple sync bytes at the right spacing
                let validPattern = true;
                for (let j = 1; j <= Math.min(3, Math.floor((combinedBuffer.length - i) / 188)); j++) {
                    if (combinedBuffer[i + (j * 188)] !== 0x47) {
                        validPattern = false;
                        break;
                    }
                }
                if (validPattern) {
                    syncIndex = i;
                    break;
                }
            }
        }
        
        if (syncIndex === -1) {
            // No valid sync pattern found, store the data for next time
            this.streamBuffers.set(streamId, combinedBuffer);
            return [];
        }
        
        if (syncIndex > 0) {
            // Remove data before the first sync byte
            combinedBuffer = combinedBuffer.slice(syncIndex);
        }
        
        // Calculate how many complete packets we have
        const completePackets = Math.floor(combinedBuffer.length / 188);
        if (completePackets === 0) {
            // Not enough data for a complete packet, store for next time
            this.streamBuffers.set(streamId, combinedBuffer);
            return [];
        }
        
        // Extract complete packets
        const packets = [];
        for (let i = 0; i < completePackets; i++) {
            const packetStart = i * 188;
            packets.push(combinedBuffer.slice(packetStart, packetStart + 188));
        }
        
        // Store any remaining data for next time
        const remainingData = combinedBuffer.slice(completePackets * 188);
        this.streamBuffers.set(streamId, remainingData);
        
        return packets;
    }
    
    /**
     * Process packets: extract PTS, validate, and enqueue in PTS order
     * @param {Array} packets - Array of MPEG-TS packets
     * @param {string} streamId - Stream identifier
     * @param {Object} packetValidator - The TS packet validator to use
     * @returns {number} - Number of processed packets
     */
    _processAndSortPackets(packets, streamId, packetValidator) {
        if (!packets || packets.length === 0) {
            return 0;
        }
        
        const ptsRange = this.streamPTSRanges.get(streamId) || { firstPTS: null, lastPTS: null };
        let processedCount = 0;
        
        for (let i = 0; i < packets.length; i++) {
            const packet = packets[i];
            this.stats.packetsProcessed++;
            
            // Basic packet validation
            if (packet[0] !== 0x47) {
                continue; // Skip invalid packets
            }
            
            // Extract packet information
            const pid = ((packet[1] & 0x1F) << 8) | packet[2];
            const payloadStart = (packet[1] & 0x40) !== 0;
            const hasAdaptationField = (packet[3] & 0x20) !== 0;
            const hasPayload = (packet[3] & 0x10) !== 0;
            
            // Find PTS in the packet if it's the start of a PES packet
            let pts = null;
            if (payloadStart && hasPayload) {
                pts = this._extractPTS(packet, hasAdaptationField);
                
                // Update PTS range for this stream
                if (pts !== null) {
                    if (ptsRange.firstPTS === null) {
                        ptsRange.firstPTS = pts;
                    }
                    ptsRange.lastPTS = pts;
                    
                    // During transitions, detect overlapping PTS ranges between streams
                    if (this.isInTransition && this.activeStreamIds.size > 1) {
                        this._detectStreamOverlap(streamId, pts);
                    }
                }
            }
            
            // Assign a global sequence number for proper ordering
            const sequence = this.outputSequence++;
            
            // Add to ordered buffer
            this.orderedPacketBuffer.push({
                packet,
                streamId,
                pts: pts,
                sequence,
                transitionSequence: this.transitionSequenceCounter
            });
            
            processedCount++;
        }
        
        // Update the PTS range for this stream
        this.streamPTSRanges.set(streamId, ptsRange);
        
        return processedCount;
    }

/**
     * Extract PTS from a TS packet if it contains a PES header
     * @param {Uint8Array} packet - TS packet data
     * @param {boolean} hasAdaptationField - Whether the packet has an adaptation field
     * @returns {number|null} - PTS value or null if not found
     */
    _extractPTS(packet, hasAdaptationField) {
        try {
            // Calculate payload offset
            const payloadOffset = hasAdaptationField ? 
                (5 + packet[4]) : 4;
            
            // Ensure we have enough bytes for a PES header
            if (packet.length < payloadOffset + 14) {
                return null;
            }
            
            // Check for PES start code (0x000001)
            if (packet[payloadOffset] !== 0x00 || 
                packet[payloadOffset + 1] !== 0x00 || 
                packet[payloadOffset + 2] !== 0x01) {
                return null;
            }
            
            // Check stream ID for video or audio
            const streamId = packet[payloadOffset + 3];
            if ((streamId < 0xE0 || streamId > 0xEF) && // Not video
                (streamId < 0xC0 || streamId > 0xDF)) { // Not audio
                return null;
            }
            
            // Check PTS_DTS_flags
            const ptsDtsFlags = (packet[payloadOffset + 7] & 0xC0) >> 6;
            if (ptsDtsFlags === 0) {
                return null; // No PTS present
            }
            
            // Extract PTS (33-bit value spread across 5 bytes)
            const p0 = packet[payloadOffset + 9];
            const p1 = packet[payloadOffset + 10];
            const p2 = packet[payloadOffset + 11];
            const p3 = packet[payloadOffset + 12];
            const p4 = packet[payloadOffset + 13];
            
            // Combine the bytes according to MPEG-TS spec
            const pts = (((p0 >> 1) & 0x07) << 30) | 
                       (p1 << 22) | 
                       (((p2 >> 1) & 0x7F) << 15) | 
                       (p3 << 7) | 
                       ((p4 >> 1) & 0x7F);
                       
            return pts;
        } catch (e) {
            // Log the error but don't let it crash the main process
            this.onLog(`Error extracting PTS: ${e.message}`);
            return null;
        }
    }
    
    /**
     * Detect overlapping PTS ranges between streams during transition
     * @param {string} currentStreamId - Current stream ID
     * @param {number} currentPTS - Current PTS value
     */
    _detectStreamOverlap(currentStreamId, currentPTS) {
        // Check all active streams except the current one
        for (const otherStreamId of this.activeStreamIds) {
            if (otherStreamId === currentStreamId) continue;
            
            const otherRange = this.streamPTSRanges.get(otherStreamId);
            if (!otherRange || otherRange.firstPTS === null) continue;
            
            // Check if the current PTS falls within the other stream's range
            if (currentPTS >= otherRange.firstPTS && 
                currentPTS <= otherRange.lastPTS) {
                this.stats.streamOverlaps++;
                this.onLog(`PTS overlap detected between streams: ${currentStreamId} and ${otherStreamId} at PTS ${currentPTS}`);
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get the current statistics
     * @returns {Object} - Statistics object
     */
    getStats() {
        return {
            ...this.stats,
            activeStreams: this.activeStreamIds.size,
            bufferedPackets: this.orderedPacketBuffer.length,
            inTransition: this.isInTransition,
            streamRanges: Object.fromEntries([...this.streamPTSRanges.entries()].map(
                ([id, range]) => [id, {
                    firstPTS: range.firstPTS,
                    lastPTS: range.lastPTS
                }]
            ))
        };
    }
    
    /**
     * Reset the handler state
     */
    reset() {
        this.streamBuffers.clear();
        this.activeStreamIds.clear();
        this.streamPTSRanges.clear();
        this.orderedPacketBuffer = [];
        this.outputSequence = 0;
        this.globalPTSOffset = 0;
        this.lastGlobalPTS = null;
        this.isInTransition = false;
        this.transitionSequenceCounter = 0;
        
        // Reset stats
        this.stats = {
            streamsReceived: 0,
            streamsSwitched: 0,
            ptsJumps: 0,
            packetsProcessed: 0,
            packetsOutputted: 0,
            discontinuities: 0,
            streamOverlaps: 0
        };
        
        this.onLog('Stream rotation handler reset');
    }
}

class MPEGTSTestHarness {
    constructor(options = {}) {
        this.TAG = 'MPEGTSTestHarness';
        this.onLog = options.onLog || console.log;
    }

    reset() {
        this.onLog(`${this.TAG}: Test harness reset complete`);
    }

    integrateWithLoader(loader) {
        // Save the original dispatch method
        const originalDispatchMethod = loader._dispatchPacketChunk.bind(loader);

        // Override the dispatch method
        loader._dispatchPacketChunk = () => {
            if (loader._packetBuffer.length === 0) return;
            // Call the original method
            originalDispatchMethod();
        };

        this.onLog(`${this.TAG}: Test harness integrated with loader`);
    }

    generateReport() {
        return {
            message: "Test harness disabled but stub methods provided for compatibility"
        };
    }
}

/**
 * TS Packet Validator - checks for valid MPEG-TS packet structure
 */
class TSPacketValidator {
    constructor(options = {}) {
        this.PACKET_SIZE = 188;
        this.SYNC_BYTE = 0x47;
        this.lastValidPID = null;
        this.continuityCounters = new Map();
        this.onLog = options.onLog || console.log;
    }

    isLikelyValidPacketStart(buffer, index) {
        // Ensure we have enough bytes to check
        if (buffer.length < index + this.PACKET_SIZE) return false;

        // Validate sync byte and spacing
        if (buffer[index] !== this.SYNC_BYTE) {
            this.logRawBytes(buffer, index, 4);
            this.onLog(`[ERROR] Invalid sync byte at ${index}: 0x${buffer[index].toString(16)}`);
            return false;
        }

        // Extract packet information
        const pid = ((buffer[index + 1] & 0x1F) << 8) | buffer[index + 2];
        const transportError = (buffer[index + 1] & 0x80) !== 0;
        const hasAdaptationField = (buffer[index + 3] & 0x20) !== 0;
        const hasPayload = (buffer[index + 3] & 0x10) !== 0;

        // Basic validation checks
        if (pid > 0x1FFF) {
            this.onLog(`[ERROR] Invalid PID value 0x${pid.toString(16)} at ${index}`);
            return false;
        }

        if (transportError) {
            this.onLog(`[ERROR] Transport error at ${index}`);
            return false;
        }

        if (!hasPayload && !hasAdaptationField) {
            return false;
        }

        // Check for adaptation field length validity
        if (hasAdaptationField) {
            const adaptationLength = buffer[index + 4];
            if (adaptationLength > 183) {
                this.onLog(`[ERROR] Invalid adaptation field length ${adaptationLength} at ${index}`);
                return false;
            }
        }

        this.lastValidPID = pid;
        return true;
    }

    validateContinuityCounter(pid, currentCounter) {
        const lastCounter = this.continuityCounters.get(pid);

        if (lastCounter !== undefined) {
            const expectedCounter = (lastCounter + 1) & 0x0F;
            if (currentCounter !== expectedCounter) {
                return false;
            }
        }

        this.continuityCounters.set(pid, currentCounter);
        return true;
    }

    hasCorruptedSectionMarker(buffer, offset) {
        const knownCorruptMarkers = [0x6c46, 0xf408];

        if (buffer.length < offset + 2) return false;

        const marker = (buffer[offset] << 8) | buffer[offset + 1];
        return knownCorruptMarkers.includes(marker);
    }

    logRawBytes(buffer, start, length) {
        const bytes = Array.from(buffer.slice(start, start + length))
            .map(b => b.toString(16).padStart(2, '0'))
            .join(' ');
    }

    logVideoPacketDetails(buffer, index, payloadStart, hasAdaptationField, continuityCounter) {
        const payloadOffset = hasAdaptationField ? 5 + buffer[index + 4] : 4;

        if (payloadStart && buffer.length >= index + payloadOffset + 4) {
            const startCode = buffer.slice(index + payloadOffset, index + payloadOffset + 4);
            if (startCode[0] === 0x00 && startCode[1] === 0x00 &&
                startCode[2] === 0x00 && startCode[3] === 0x01) {
                // Valid H.264 start code found
            }
        }
    }
}

/**
 * MPEG-TS Buffer - manages and processes MPEG-TS data chunks
 */
class MPEGTSBuffer {
    constructor(onLog) {
        this.PACKET_SIZE = 188;
        this.SYNC_BYTE = 0x47;
        this.MAX_BUFFER_SIZE = 1024 * 1024; // 1MB
        this.buffer = new Uint8Array(0);
        this.onLog = onLog || (() => {});
        this.validator = new TSPacketValidator({ onLog: this.onLog });

        // Add validation statistics
        this.stats = {
            totalPacketsProcessed: 0,
            validPackets: 0,
            invalidPackets: 0,
            pidDistribution: new Map(),
            adaptationFieldCount: 0,
            invalidSyncByteCount: 0
        };
    }

addChunk(chunk) {
        if (!chunk || chunk.length === 0) return null;

        const inputChunk = (chunk instanceof Uint8Array) ? chunk : new Uint8Array(chunk);

        // Append new data to existing buffer
        let newBuffer = new Uint8Array(this.buffer.length + inputChunk.length);
        newBuffer.set(this.buffer, 0);
        newBuffer.set(inputChunk, this.buffer.length);
        this.buffer = newBuffer;

        // Find first valid sync byte
        let syncIndex = this.findSyncByte(this.buffer);
        if (syncIndex === -1) {
            if (this.buffer.length > this.MAX_BUFFER_SIZE) {
                this.onLog('[MPEGTSBuffer] Buffer overflow, resetting');
                this.buffer = new Uint8Array(0);
            }
            return null;
        }

        // Trim before first valid sync byte
        if (syncIndex > 0) {
            this.buffer = this.buffer.slice(syncIndex);
        }

        // Process only full packets
        let completePackets = Math.floor(this.buffer.length / this.PACKET_SIZE);
        if (completePackets === 0) return null;

        let packetsData = this.buffer.slice(0, completePackets * this.PACKET_SIZE);
        this.buffer = this.buffer.slice(completePackets * this.PACKET_SIZE);

        return this.validatePackets(packetsData);
    }

    validatePackets(packets) {
        const validPackets = [];
        
        try {
            for (let i = 0; i < packets.length; i += this.PACKET_SIZE) {
                // Ensure we have enough bytes for a complete packet
                if (i + this.PACKET_SIZE > packets.length) {
                    break;
                }
                
                this.stats.totalPacketsProcessed++;
                const currentPacket = packets.slice(i, i + this.PACKET_SIZE);
                
                // Basic check for sync byte
                if (currentPacket[0] !== this.SYNC_BYTE) {
                    this.stats.invalidPackets++;
                    this.stats.invalidSyncByteCount++;
                    continue;
                }

                // Extract packet information
                const pid = ((currentPacket[1] & 0x1F) << 8) | currentPacket[2];
                const hasAdaptationField = (currentPacket[3] & 0x20) !== 0;
                const hasPayload = (currentPacket[3] & 0x10) !== 0;
                const payloadStart = (currentPacket[1] & 0x40) !== 0;
                
                // Update statistics
                this.stats.validPackets++;
                this.stats.pidDistribution.set(pid, (this.stats.pidDistribution.get(pid) || 0) + 1);
                if (hasAdaptationField) {
                    this.stats.adaptationFieldCount++;
                }
                
                validPackets.push(currentPacket);
            }

            return validPackets.length > 0 ? validPackets : null;
        } catch (error) {
            this.onLog(`[ERROR] Error in validatePackets: ${error.message}`);
            return validPackets.length > 0 ? validPackets : null;
        }
    }

    findSyncByte(buffer) {
        // Check every possible position within the buffer
        for (let i = 0; i <= buffer.length - this.PACKET_SIZE; i++) {
            if (buffer[i] === this.SYNC_BYTE) {
                // Check if we have multiple sync bytes at the expected spacing
                let validSyncCount = 1;
                
                // Look for up to 3 more sync bytes at PACKET_SIZE intervals
                for (let j = 1; j <= 3; j++) {
                    const nextSyncPos = i + (j * this.PACKET_SIZE);
                    if (nextSyncPos < buffer.length && buffer[nextSyncPos] === this.SYNC_BYTE) {
                        validSyncCount++;
                    } else {
                        break;
                    }
                }
                
                // If we found at least 2 sync bytes at the right spacing, it's likely a valid start
                if (validSyncCount >= 2) {
                    return i;
                }
            }
        }
        
        // Fallback: return the first sync byte we find, even if it doesn't have follow-ups
        for (let i = 0; i <= buffer.length - this.PACKET_SIZE; i++) {
            if (buffer[i] === this.SYNC_BYTE) {
                return i;
            }
        }
        
        return -1;
    }
}

/**
 * Packet Logger - tracks and analyzes MPEG-TS packet details
 */
class PacketLogger {
    constructor(onLog) {
        this.packetCount = 0;
        this.onLog = onLog || (() => {});

        // PTS tracking & estimation
        this.lastValidPTS = null;
        this.prevPTS = null;
        this.estimatedFrameDuration = 3003;
        this.wraparoundOffset = 0;
        
        // Program info tracking
        this.videoPID = 256;
        this.lastVideoPTS = null;
        
        // Debug stats
        this.debugStats = {
            totalPackets: 0,
            videoPIDPackets: 0,
            validPTS: 0,
            notPESHeader: 0,
            noPTS: 0,
            pesStarts: 0,
            pts: []
        };
    }

    logPacket(packet, timeReceived) {
        if (!packet || !(packet instanceof Uint8Array) || packet.length !== 188) {
            return;
        }

        this.packetCount++;
        this.debugStats.totalPackets++;

        if (packet[0] !== 0x47) return;

        // Extract packet info
        const pid = ((packet[1] & 0x1F) << 8) | packet[2];
        const payloadUnitStart = (packet[1] & 0x40) !== 0;
        const hasAdaptationField = (packet[3] & 0x20) !== 0;
        const hasPayload = (packet[3] & 0x10) !== 0;

        if (pid === this.videoPID && hasPayload) {
            this.debugStats.videoPIDPackets++;
            
            // Calculate payload start
            let payloadOffset = 4;
            let randomAccessIndicator = 0;
            
            if (hasAdaptationField) {
                if (packet.length < 5) return;
                const adaptationFieldLength = packet[4];
                
                // Check for random access indicator in adaptation field
                if (adaptationFieldLength > 0 && packet.length >= 6) {
                    // Adaptation field flags byte
                    const adaptationFlags = packet[5];
                    randomAccessIndicator = (adaptationFlags & 0x40) >> 6;
                }
                
                if (adaptationFieldLength > 183) return;
                payloadOffset = 5 + adaptationFieldLength;
            }

            if (payloadOffset >= packet.length) return;

            if (payloadUnitStart) {
                this.debugStats.pesStarts++;
                const payload = packet.slice(payloadOffset);
                
                let pts = this._extractPTS(payload);
                if (pts !== null) {
                    this.debugStats.validPTS++;
                    pts = this._handleWraparound(pts);
                    this.lastValidPTS = pts;
                    this.lastVideoPTS = pts;
                    
                    this.debugStats.pts.push(pts);
                    if (this.debugStats.pts.length > 5) {
                        this.debugStats.pts.shift();
                    }
                }
            }
        }
    }

    _extractPTS(payload) {
        // Ensure there is enough data and a valid PES start
        if (payload.length < 14) {
            this.debugStats.noPTS++;
            return null;
        }
        
        if (payload[0] !== 0x00 || payload[1] !== 0x00 || payload[2] !== 0x01) {
            this.debugStats.notPESHeader++;
            return null;
        }
        
        // Check for video stream ID - could be 0xE0-0xEF for video
        if (payload[3] < 0xE0 || payload[3] > 0xEF) {
            this.debugStats.notPESHeader++;
            return null;
        }
        
        const ptsDtsFlags = (payload[7] & 0xC0) >> 6;
        if (ptsDtsFlags === 0) {
            this.debugStats.noPTS++;
            return null;
        }

        // The five PTS bytes are located at indexes 9 to 13.
        const p0 = payload[9];
        const p1 = payload[10];
        const p2 = payload[11];
        const p3 = payload[12];
        const p4 = payload[13];
        
        // Extract the 33-bit PTS, handling marker bits correctly
        const pts =
            (((p0 >> 1) & 0x07) << 30) | // Bits 32-30
            (p1 << 22) |                 // Bits 29-22
            (((p2 >> 1) & 0x7F) << 15) | // Bits 21-15
            (p3 << 7) |                  // Bits 14-7
            ((p4 >> 1) & 0x7F);          // Bits 6-0
        
        return pts;
    }

    _handleWraparound(pts) {
        if (this.lastValidPTS !== null && pts < this.lastValidPTS) {
            const ptsDrop = this.lastValidPTS - pts;
            if (ptsDrop > 4294967296) {
                this.wraparoundOffset += 8589934592;
            }
            pts += this.wraparoundOffset;
        }
        return pts;
    }
}

/**
 * MPEGTSTestHarness - Class for monitoring and testing MPEG-TS stream health
 */
class MPEGTSTestHarness {
    constructor(options = {}) {
        this.TAG = 'MPEGTSTestHarness';
        this.onLog = options.onLog || (() => {});

        // Metrics tracking
        this.metrics = {
            packetsReceived: 0,
            syncLossCount: 0,
            ccErrorCount: 0,
            ptsJumpCount: 0,
            streamSwitches: 0,
            lastPTSValue: null,
            ptsTimestamps: [],
            packetGaps: []
        };

        // Reference to the loader
        this.loader = null;

        // Sampling rate
        this.sampleRate = options.sampleRate || 20; // 1 in X packets
        this.maxSampleHistory = options.maxSampleHistory || 100;
    }

    reset() {
        this.metrics = {
            packetsReceived: 0,
            syncLossCount: 0,
            ccErrorCount: 0,
            ptsJumpCount: 0,
            streamSwitches: 0,
            lastPTSValue: null,
            ptsTimestamps: [],
            packetGaps: []
        };

        this.onLog('Test harness reset complete');
    }

    integrateWithLoader(loader) {
        this.loader = loader;
        this.onLog('Test harness integrated with loader');
    }

    recordPacketReceived(packet, timestamp) {
        this.metrics.packetsReceived++;

        // Sample only a portion of packets for performance
        if (this.metrics.packetsReceived % this.sampleRate !== 0) {
            return;
        }

        // Check for sync byte
        if (packet[0] !== 0x47) {
            this.metrics.syncLossCount++;
        }

        // Extract PID and continuity counter
        const pid = ((packet[1] & 0x1F) << 8) | packet[2];
        const cc = packet[3] & 0x0F;

        // Record timestamp if it's a video packet with PTS
        this._extractAndRecordPTS(packet, timestamp);
    }

    _extractAndRecordPTS(packet, timestamp) {
        // Extract PTS (simplified implementation)
        try {
            const payloadStart = (packet[1] & 0x40) !== 0;
            const hasAdaptationField = (packet[3] & 0x20) !== 0;
            const hasPayload = (packet[3] & 0x10) !== 0;

            if (!payloadStart || !hasPayload) {
                return;
            }

            // Calculate payload offset
            const payloadOffset = hasAdaptationField ?
                (5 + packet[4]) : 4;

            // Ensure we have enough bytes for a PES header
            if (packet.length < payloadOffset + 14) {
                return;
            }

            // Check for PES start code (0x000001)
            if (packet[payloadOffset] !== 0x00 ||
                packet[payloadOffset + 1] !== 0x00 ||
                packet[payloadOffset + 2] !== 0x01) {
                return;
            }

            // Check stream ID for video
            const streamId = packet[payloadOffset + 3];
            if (streamId < 0xE0 || streamId > 0xEF) {
                return; // Not video
            }

            // Check PTS_DTS_flags
            const ptsDtsFlags = (packet[payloadOffset + 7] & 0xC0) >> 6;
            if (ptsDtsFlags === 0) {
                return; // No PTS present
            }

            // Extract PTS (33-bit value spread across 5 bytes)
            const p0 = packet[payloadOffset + 9];
            const p1 = packet[payloadOffset + 10];
            const p2 = packet[payloadOffset + 11];
            const p3 = packet[payloadOffset + 12];
            const p4 = packet[payloadOffset + 13];

            // Combine the bytes according to MPEG-TS spec
            const pts = (((p0 >> 1) & 0x07) << 30) |
                       (p1 << 22) |
                       (((p2 >> 1) & 0x7F) << 15) |
                       (p3 << 7) |
                       ((p4 >> 1) & 0x7F);

            // Check for PTS jumps
            if (this.metrics.lastPTSValue !== null) {
                const ptsDiff = Math.abs(pts - this.metrics.lastPTSValue);
                // Check for unexpected jumps (over 45000 in 90kHz clock is ~0.5 sec)
                if (ptsDiff > 45000 && ptsDiff < 8589934592) { // Ignore wraparound
                    this.metrics.ptsJumpCount++;
                }
            }

            this.metrics.lastPTSValue = pts;

            // Record PTS with timestamp
            this.metrics.ptsTimestamps.push({
                pts: pts,
                timestamp: timestamp
            });

            // Keep history within bounds
            if (this.metrics.ptsTimestamps.length > this.maxSampleHistory) {
                this.metrics.ptsTimestamps.shift();
            }

        } catch (e) {
            // Silently ignore extraction errors
        }
    }

    recordStreamSwitch() {
        this.metrics.streamSwitches++;
    }

    generateReport() {
        // Calculate stream health indicators
        let ptsJitterMs = 0;
        let bitrateBps = 0;
        let streamSwitchRate = 0;

        // Calculate PTS jitter if we have enough samples
        if (this.metrics.ptsTimestamps.length > 5) {
            try {
                // Measure PTS vs wall clock consistency
                const ptsRatios = [];
                for (let i = 1; i < this.metrics.ptsTimestamps.length; i++) {
                    const curr = this.metrics.ptsTimestamps[i];
                    const prev = this.metrics.ptsTimestamps[i-1];

                    const ptsDiff = (curr.pts - prev.pts) & 0x1FFFFFFFF; // Handle 33-bit wraparound
                    const timeDiff = curr.timestamp - prev.timestamp;

                    if (timeDiff > 0) {
                        // PTS is in 90kHz clock, convert to ms
                        const expectedTime = ptsDiff / 90; // ms
                        const ratio = expectedTime / timeDiff;
                        ptsRatios.push(ratio);
                    }
                }

                // Calculate average and standard deviation
                if (ptsRatios.length > 0) {
                    const avgRatio = ptsRatios.reduce((a, b) => a + b, 0) / ptsRatios.length;
                    const variance = ptsRatios.reduce((a, b) => a + Math.pow(b - avgRatio, 2), 0) / ptsRatios.length;
                    ptsJitterMs = Math.sqrt(variance) * 100; // Convert to ms scale
                }
            } catch (e) {
                this.onLog(`Error calculating jitter: ${e.message}`);
            }
        }

        // Get additional metrics from loader if available
        let rotationStats = {};
        let streamCount = 0;

        if (this.loader) {
            if (typeof this.loader.getStreamHealthReport === 'function') {
                try {
                    const healthReport = this.loader.getStreamHealthReport();
                    rotationStats = healthReport.rotationStats || {};
                    streamCount = healthReport.streamCount || 0;

                    // Calculate bitrate from bytes received if possible
                    if (healthReport.receivedBytes &&
                        this.metrics.ptsTimestamps.length >= 2) {
                        const firstTs = this.metrics.ptsTimestamps[0].timestamp;
                        const lastTs = this.metrics.ptsTimestamps[this.metrics.ptsTimestamps.length - 1].timestamp;
                        const durationSec = (lastTs - firstTs) / 1000;

                        if (durationSec > 0) {
                            bitrateBps = Math.round((healthReport.receivedBytes * 8) / durationSec);
                        }
                    }
                } catch (e) {
                    this.onLog(`Error getting loader health report: ${e.message}`);
                }
            }
        }

        return {
            packetsReceived: this.metrics.packetsReceived,
            syncLossRate: this.metrics.packetsReceived > 0 ?
                (this.metrics.syncLossCount / this.metrics.packetsReceived) : 0,
            ptsJumpCount: this.metrics.ptsJumpCount,
            streamSwitches: this.metrics.streamSwitches,
            estimatedBitrate: bitrateBps,
            ptsJitterMs: ptsJitterMs,
            activeStreamCount: streamCount,
            ...rotationStats
        };
    }
}

class WebTransportLoader extends BaseLoader {
    constructor() {
        super('webtransport-loader');
        this.TAG = 'WebTransportLoader';

        this._needStash = true;
        this._transport = null;
        this._reader = null;
        this._requestAbort = false;
        this._receivedLength = 0;
        this.debugCounter = 0;
        this._connectionState = 'idle'; // 'idle', 'connecting', 'connected', 'reconnecting', 'failed'
        this._readerState = 'none';     // 'none', 'acquiring', 'active', 'releasing'

        // Configure settings
        this.config = {
            enableStreamRotationHandler: true,
            minBufferTime: 1000,     // Min time to buffer before output
            maxBufferTime: 5000,     // Max time to buffer before forcing output
            packetFlushThreshold: 100,  // Number of packets per chunk
            maxConcurrentStreams: 5,    // Max concurrent streams to handle
            enableDetailedLogging: false
        };

        // Initialize with bound logging function
        const logFunction = (msg) => this.config.enableDetailedLogging ? Log.v(this.TAG, msg) : null;
        this.tsBuffer = new MPEGTSBuffer(logFunction);
        this.packetLogger = new PacketLogger(logFunction);
        this.packetValidator = new TSPacketValidator({ onLog: logFunction });

        // Initialize stream rotation handler
        this.streamRotationHandler = new StreamRotationHandler({
            onLog: logFunction,
            maxBufferSize: 5 * 1024 * 1024,  // 5MB per stream
            minBufferTime: this.config.minBufferTime,
            maxBufferTime: this.config.maxBufferTime,
            flushThreshold: this.config.packetFlushThreshold
        });

        // Initialize testHarness properly - now using our new class
        //this.testHarness = new MPEGTSTestHarness({
        //    onLog: (msg) => { Log.v(this.TAG, msg); }
        //});
	this.testHarness = {
	    reset: () => {},
	    integrateWithLoader: () => {},
	    generateReport: () => { return {}; }
	};

        // Stream tracking
        this._activeStreams = new Map();  // streamId -> {reader, startTime, status}
        this._streamIdCounter = 0;
        this._lastFlushTime = 0;
        
        // Output buffering
        this.PACKETS_PER_CHUNK = this.config.packetFlushThreshold;
        this._packetBuffer = [];
        this._outputBuffer = [];
        
        // Internal state
        this._isInitialized = false;
        this._connectionEstablished = false;
        
        // Bind methods - core methods first
        this._readChunks = this._readChunks.bind(this);
        this._processPackets = this._processPackets.bind(this);
        this._dispatchPacketChunk = this._dispatchPacketChunk.bind(this);
        
        // Then stream rotation related methods
        if (typeof this._monitorIncomingStreams === 'function') {
            this._monitorIncomingStreams = this._monitorIncomingStreams.bind(this);
        }
        
        if (typeof this._processStreamData === 'function') {
            this._processStreamData = this._processStreamData.bind(this);
        }
        
        if (typeof this._handleProcessedPackets === 'function') {
            this._handleProcessedPackets = this._handleProcessedPackets.bind(this);
        }
        
        // Bind diagnostic method
        this.advancedStreamDiagnosis = this.advancedStreamDiagnosis.bind(this);
    }

    static isSupported() {
        try {
            return typeof self.WebTransport !== 'undefined';
        } catch (e) {
            return false;
        }
    }

async open(dataSource) {
    try {
        if (!dataSource.url.startsWith('https://')) {
            throw new Error('WebTransport requires HTTPS URL');
        }

        Log.v(this.TAG, `Opening WebTransport connection to ${dataSource.url}`);
        this._dataSource = dataSource;
        this._requestAbort = false;

        // IMPORTANT: Set initial connection state
        this._connectionState = 'connecting';
        this._readerState = 'none';

        // Create WebTransport connection
        this._transport = new WebTransport(dataSource.url);
        
        // Wait for the connection to be ready
        await this._transport.ready;
        this._connectionEstablished = true;
        
        Log.v(this.TAG, "WebTransport connection established successfully");
        
        // Set up handlers for connection events
        this._transport.closed
            .then(info => {
                // Update state when connection closes
                this._connectionState = 'idle';
                this._onConnectionClosed(info);
            })
            .catch(error => {
                // Update state on connection error
                this._connectionState = 'failed';
                this._onConnectionError(error);
            });

        // Update connection state
        this._connectionState = 'connected';

        // Get incoming streams directly after connection established
        this._readerState = 'acquiring';
        const incomingStreams = this._transport.incomingUnidirectionalStreams;
        const streamReader = incomingStreams.getReader();
        
        // Read the first stream
        const { value: stream, done } = await streamReader.read();
        
        // Always release the stream reader
        try {
            streamReader.releaseLock();
        } catch (e) {
            Log.w(this.TAG, `Error releasing initial stream reader: ${e.message}`);
        }
        
        if (done || !stream) {
            throw new Error('No incoming stream received');
        }
        
        // Initialize the reader with the first stream
        this._reader = stream.getReader();
        this._readerState = 'active';
        Log.v(this.TAG, "First stream obtained successfully");
        
        // Start heartbeat to keep connection alive
        this._startHeartbeat();
        
        // Schedule periodic connection state checks
        this._connectionCheckInterval = setInterval(() => {
            this._checkConnectionState();
        }, 5000);
        
        // Start reading from the stream
        this._status = LoaderStatus.kBuffering;
        this._readChunks();
        
    } catch (e) {
        Log.e(this.TAG, `Error opening WebTransport: ${e.message}`);
        this._status = LoaderStatus.kError;
        this._connectionState = 'failed';
        this._readerState = 'none';
        if (this._onError) {
            this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
        }
    }
}

async _getNextStream() {
    try {
        // Check that we have a valid transport that's fully initialized
        if (!this._transport) {
            Log.e(this.TAG, "Transport is null, cannot get next stream");
            return false;
        }

        if (this._transport.closed) {
            Log.e(this.TAG, "Transport is closed, cannot get next stream");
            return false;
        }

        if (!this._connectionEstablished) {
            Log.e(this.TAG, "Connection not fully established yet");
            return false;
        }

        // Release the previous reader if any
        this._reader = null;

        Log.v(this.TAG, "Attempting to get next stream");

        try {
            // Get the incoming streams reader
            const incomingStreams = this._transport.incomingUnidirectionalStreams;
            const streamReader = incomingStreams.getReader();

            // Try to read the next stream
            const { value: stream, done } = await streamReader.read();

            // Release the streams reader
            streamReader.releaseLock();

            if (done) {
                Log.v(this.TAG, "No more streams available");
                return false;
            }

            if (!stream) {
                Log.w(this.TAG, "Received null stream");
                return false;
            }

            // Got a new stream, create reader
            this._reader = stream.getReader();

            // Update statistics
            const streamId = `stream-${this._streamIdCounter++}`;
            Log.v(this.TAG, `New stream received: ${streamId}`);

            if (this.streamRotationHandler) {
                this.streamRotationHandler.registerStream(streamId);
            }

            if (this.testHarness && typeof this.testHarness.recordStreamSwitch === 'function') {
                this.testHarness.recordStreamSwitch();
            }

            return true;
        } catch (streamError) {
            Log.e(this.TAG, `Error getting stream: ${streamError.message}`);
            return false;
        }
    } catch (e) {
        Log.e(this.TAG, `Error in _getNextStream: ${e.message}`);
        return false;
    }
}

_startHeartbeat() {
    // Clear any existing heartbeat interval
    if (this._heartbeatInterval) {
        clearInterval(this._heartbeatInterval);
        this._heartbeatInterval = null;
    }
    
    // Set heartbeat failure count
    this._heartbeatFailures = 0;
    const MAX_CONSECUTIVE_FAILURES = 3;
    
    // Send a heartbeat every 5 seconds to keep the connection alive
    this._heartbeatInterval = setInterval(async () => {
        try {
            if (!this._transport || this._transport.closed) {
                this._heartbeatFailures++;
                Log.w(this.TAG, `Cannot send heartbeat: transport closed (failure ${this._heartbeatFailures}/${MAX_CONSECUTIVE_FAILURES})`);
                
                // If we have too many consecutive failures, try to reconnect
                if (this._heartbeatFailures >= MAX_CONSECUTIVE_FAILURES) {
                    Log.w(this.TAG, "Too many heartbeat failures, attempting reconnection");
                    clearInterval(this._heartbeatInterval);
                    this._heartbeatInterval = null;
                    
                    // Trigger transport reconnection
                    this._handleTransportClosure("Multiple heartbeat failures");
                }
                return;
            }
            
            // Reset failure count on successful check
            this._heartbeatFailures = 0;
            
            // Create a bidirectional stream as a heartbeat
            try {
                const stream = await this._transport.createBidirectionalStream();
                const writer = stream.writable.getWriter();

                // Send a simple ping message
                const encoder = new TextEncoder();
                const pingData = encoder.encode('ping');
                await writer.write(pingData);

                // Close the stream properly
                await writer.close();

                // Log success with lower frequency to avoid cluttering logs
                if (Math.random() < 0.1) { // Only log approximately 10% of heartbeats
                    Log.v(this.TAG, "Heartbeat sent to keep connection alive");
                }
            } catch (streamError) {
                this._heartbeatFailures++;
                Log.w(this.TAG, `Heartbeat stream error: ${streamError.message} (failure ${this._heartbeatFailures}/${MAX_CONSECUTIVE_FAILURES})`);
                
                // If we have multiple failures creating streams, the connection might be failing
                if (this._heartbeatFailures >= MAX_CONSECUTIVE_FAILURES) {
                    Log.w(this.TAG, "Too many heartbeat failures, attempting reconnection");
                    clearInterval(this._heartbeatInterval);
                    this._heartbeatInterval = null;
                    
                    // Trigger transport reconnection
                    this._handleTransportClosure("Multiple heartbeat failures");
                }
            }
        } catch (e) {
            this._heartbeatFailures++;
            Log.w(this.TAG, `Heartbeat error: ${e.message} (failure ${this._heartbeatFailures}/${MAX_CONSECUTIVE_FAILURES})`);
            
            // If we have too many consecutive failures, try to reconnect
            if (this._heartbeatFailures >= MAX_CONSECUTIVE_FAILURES) {
                Log.w(this.TAG, "Too many heartbeat failures, attempting reconnection");
                clearInterval(this._heartbeatInterval);
                this._heartbeatInterval = null;
                
                // Trigger transport reconnection
                this._handleTransportClosure("Multiple heartbeat failures");
            }
        }
    }, 5000);
}

async _onConnectionClosed(info) {
    // Don't handle closure if we're already in the process of aborting
    if (this._requestAbort) {
        Log.v(this.TAG, "Connection closed during abort, ignoring");
        return;
    }
    
    Log.w(this.TAG, `WebTransport connection closed: ${JSON.stringify(info)}`);
    
    // Clean up heartbeat timer
    if (this._heartbeatInterval) {
        clearInterval(this._heartbeatInterval);
        this._heartbeatInterval = null;
    }
    
    // Try to handle the closure and reconnect
    const reconnected = await this._handleTransportClosure(
        `Connection closed with code ${info.closeCode}, reason: ${info.reason}`
    );
    
    // If we couldn't reconnect and we're not already in error state, report error
    if (!reconnected && this._status !== LoaderStatus.kError && this._status !== LoaderStatus.kComplete) {
        this._status = LoaderStatus.kError;
        if (this._onError) {
            this._onError(LoaderErrors.CONNECTION_ERROR, {
                code: info.closeCode || -1,
                msg: `WebTransport connection closed: ${info.reason || "No reason provided"}`
            });
        }
    }
}

_onConnectionError(error) {
    // Handle error closure
    if (this._status !== LoaderStatus.kComplete && this._status !== LoaderStatus.kError) {
        this._status = LoaderStatus.kError;
        if (this._onError) {
            this._onError(LoaderErrors.CONNECTION_ERROR, {
                code: error.code || -1,
                msg: `WebTransport connection error: ${error.message}`
            });
        }
    }
}

async _monitorStreams(streamReader) {
    try {
        Log.v(this.TAG, "Monitoring for additional incoming streams");

        while (!this._requestAbort) {
            try {
                // Validate transport before each read
                if (!this._transport || this._transport.closed) {
                    Log.e(this.TAG, "Transport is closed or null during stream monitoring");
                    break;
                }

                const { value: stream, done } = await streamReader.read();

                if (done) {
                    Log.v(this.TAG, "No more incoming streams available");
                    break;
                }

                if (!stream) {
                    Log.w(this.TAG, "Received null stream, continuing");
                    continue;
                }

                Log.v(this.TAG, "New stream received, processing...");

                // Process the new stream (implementation depends on your rotation handler)
                // For now, we'll just use the most recent stream
                this._reader = stream.getReader();

                // If you implement rotation, you would do something like:
                // this.streamRotationHandler.addStream(stream.getReader());
            } catch (streamError) {
                if (this._requestAbort) break;

                Log.e(this.TAG, `Error reading stream: ${streamError.message}`);
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    } catch (e) {
        Log.e(this.TAG, `Stream monitor error: ${e.message}`);
    }
}

async _checkConnectionState() {
    // Skip checks if we're shutting down
    if (this._requestAbort) {
        return true;
    }
    
    // Check if transport is invalid or closed but we think it's active
    if ((this._transport === null || this._transport.closed) && 
        this._status !== LoaderStatus.kError && 
        this._status !== LoaderStatus.kComplete) {
        
        Log.w(this.TAG, "WebTransport connection has closed unexpectedly");
        
        // Try to reconnect
        const reconnected = await this._handleTransportClosure("Connection state check failed");
        
        if (!reconnected) {
            // Set proper status if reconnection failed
            this._status = LoaderStatus.kError;
            
            // Notify error handler
            if (this._onError) {
                this._onError(LoaderErrors.CONNECTION_ERROR, { 
                    code: -1, 
                    msg: "WebTransport connection closed unexpectedly and reconnection failed" 
                });
            }
            
            return false;
        }
        
        return true;
    }
    
    // Check for inactivity
    const now = Date.now();
    if (this.lastPacketTime && (now - this.lastPacketTime > 30000)) {
        // No data received for 30 seconds
        Log.w(this.TAG, `No data received for ${Math.floor((now - this.lastPacketTime)/1000)} seconds, checking connection`);
        
        // Try to reconnect if there's excessive inactivity
        const reconnected = await this._handleTransportClosure("Excessive inactivity");
        
        if (!reconnected) {
            // Set proper status if reconnection failed
            this._status = LoaderStatus.kError;
            
            // Notify error handler
            if (this._onError) {
                this._onError(LoaderErrors.CONNECTION_ERROR, { 
                    code: -1, 
                    msg: "Connection inactive for too long and reconnection failed" 
                });
            }
            
            return false;
        }
    }
    
    return true;
}

_cleanup() {
    // Nullify references to avoid using closed connections
    this._reader = null;
    
    // Mark all streams as ended
    for (const [streamId, streamData] of this._activeStreams.entries()) {
        streamData.status = 'ended';
        if (this.streamRotationHandler) {
            this.streamRotationHandler.endStream(streamId);
        }
    }
    
    // Flush any pending data
    if (this._packetBuffer.length > 0) {
        this._dispatchPacketChunk();
    }
    
    if (this._outputBuffer.length > 0) {
        this._flushOutputBuffer();
    }
    
    Log.v(this.TAG, "Connection resources cleaned up");
}

async _processStreamData(streamId, reader) {
    if (!reader) {
        const streamData = this._activeStreams.get(streamId);
        if (!streamData || !streamData.reader) {
            Log.e(this.TAG, `No reader available for stream ${streamId}`);
            return;
        }
        reader = streamData.reader;
    }
    
    Log.v(this.TAG, `Processing stream ${streamId}`);
    
    try {
        let pendingData = new Uint8Array(0);
        
        while (!this._requestAbort) {
            try {
                if (!this._transport || this._transport.closed) {
                    Log.w(this.TAG, `Transport closed during stream ${streamId} processing`);
                    break;
                }
                
                const { value, done } = await reader.read();
                
                if (this._requestAbort) break;
                
                if (done) {
                    Log.v(this.TAG, `Stream ${streamId} ended`);
                    
                    if (pendingData.length >= 188) {
                        const validLength = Math.floor(pendingData.length / 188) * 188;
                        
                        if (this.config.enableStreamRotationHandler) {
                            const result = this.streamRotationHandler.processStreamData(
                                streamId, pendingData.slice(0, validLength), this.packetValidator
                            );
                            
                            if (result && result.shouldFlush) {
                                const packets = this.streamRotationHandler.flushBuffers();
                                this._handleProcessedPackets(packets);
                            }
                        } else {
                            const packets = this.tsBuffer.addChunk(pendingData.slice(0, validLength));
                            if (packets) {
                                this._processPackets(packets);
                            }
                        }
                    }
                    
                    const streamData = this._activeStreams.get(streamId);
                    if (streamData) {
                        streamData.status = 'ended';
                        this._activeStreams.set(streamId, streamData);
                    }
                    
                    if (this.config.enableStreamRotationHandler) {
                        const remainingPackets = this.streamRotationHandler.endStream(streamId);
                        if (remainingPackets && remainingPackets.length > 0) {
                            this._handleProcessedPackets(remainingPackets);
                        }
                    }
                    
                    break;
                }
                
                if (value) {
                    let chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
                    
                    const streamData = this._activeStreams.get(streamId);
                    if (streamData) {
                        streamData.bytesReceived += chunk.length;
                        this._activeStreams.set(streamId, streamData);
                    }
                    
                    if (pendingData.length > 0) {
                        const newBuffer = new Uint8Array(pendingData.length + chunk.length);
                        newBuffer.set(pendingData, 0);
                        newBuffer.set(chunk, pendingData.length);
                        chunk = newBuffer;
                        pendingData = new Uint8Array(0);
                    }
                    
                    const syncIndex = this._findAlignedSyncByte(chunk);
                    
                    if (syncIndex === -1) {
                        pendingData = chunk;
                        continue;
                    } else if (syncIndex > 0) {
                        chunk = chunk.slice(syncIndex);
                    }
                    
                    const validLength = Math.floor(chunk.length / 188) * 188;
                    
                    if (validLength > 0) {
                        if (this.config.enableStreamRotationHandler) {
                            const result = this.streamRotationHandler.processStreamData(
                                streamId, chunk.slice(0, validLength), this.packetValidator
                            );
                            
                            if (result && streamData) {
                                streamData.packetsProcessed += result.packetCount || 0;
                                this._activeStreams.set(streamId, streamData);
                            }
                            
                            if (result && result.shouldFlush) {
                                const packets = this.streamRotationHandler.flushBuffers();
                                this._handleProcessedPackets(packets);
                            }
                        } else {
                            const packets = this.tsBuffer.addChunk(chunk.slice(0, validLength));
                            if (packets) {
                                this._processPackets(packets);
                                
                                if (streamData) {
                                    streamData.packetsProcessed += packets.length;
                                    this._activeStreams.set(streamId, streamData);
                                }
                            }
                        }
                    }
                    
                    if (validLength < chunk.length) {
                        pendingData = chunk.slice(validLength);
                    }
                }
            } catch (readError) {
                if (this._requestAbort) {
                    break;
                }
                
                Log.e(this.TAG, `Error reading from stream ${streamId}: ${readError.message}`);
                
                const streamData = this._activeStreams.get(streamId);
                if (streamData) {
                    streamData.status = 'error';
                    streamData.error = readError.message;
                    this._activeStreams.set(streamId, streamData);
                }
                
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
    } catch (e) {
        if (!this._requestAbort) {
            Log.e(this.TAG, `Error processing stream ${streamId}: ${e.message}`);
            
            const streamData = this._activeStreams.get(streamId);
            if (streamData) {
                streamData.status = 'error';
                streamData.error = e.message;
                this._activeStreams.set(streamId, streamData);
            }
        }
    }
}

_handleProcessedPackets(packets) {
    if (!packets || packets.length === 0) {
        return;
    }
    
    const now = Date.now();
    let validPackets = 0;
    
    for (let i = 0; i < packets.length; i++) {
        const packet = packets[i];
        
        if (packet instanceof Uint8Array && packet.length === 188) {
            if (packet[0] !== 0x47) {
                continue; // Skip invalid packets
            }
            
            this._receivedLength += packet.length;
            this.packetLogger.logPacket(packet, now);
            this._packetBuffer.push(packet);
            validPackets++;
        }
    }
    
    if (validPackets > 0) {
        this.lastPacketTime = Date.now();
        
        if (this._packetBuffer.length >= this.PACKETS_PER_CHUNK) {
            this._dispatchPacketChunk();
        }
    }
}

_flushOutputBuffer() {
    if (this._outputBuffer.length === 0) {
        return false;
    }
    
    const packetsToDispatch = this._outputBuffer.splice(0, 
        Math.min(this._outputBuffer.length, this.PACKETS_PER_CHUNK * 2));
    
    // Combine packets into a single chunk
    const totalLength = packetsToDispatch.reduce((sum, packet) => sum + packet.length, 0);
    const chunk = new Uint8Array(totalLength);
    
    let offset = 0;
    packetsToDispatch.forEach(packet => {
        chunk.set(packet, offset);
        offset += packet.length;
    });
    
    // Update total received length
    this._receivedLength += totalLength;
    
    // Send to demuxer
    if (this._onDataArrival) {
        this._onDataArrival(chunk.buffer, 0, totalLength);
    }
    
    return true;
}

_processPackets(packets) {
    if (!packets || !Array.isArray(packets)) return;
                
    const now = Date.now();
    let validPackets = [];
    
    packets.forEach(packet => {
        if (packet instanceof Uint8Array && packet.length === 188) {
            // Validate individual packet
            if (packet[0] !== 0x47) {
                return; // Skip invalid packet
            }
            
            this._receivedLength += packet.length;
            this.packetLogger.logPacket(packet, now);
            validPackets.push(packet);
        }
    });
    
    // Only add valid packets to the buffer
    if (validPackets.length > 0) {
        // Update the last packet time
        this.lastPacketTime = Date.now();
        
        this._packetBuffer.push(...validPackets);
        
        // When we have enough packets, dispatch them as a chunk
        if (this._packetBuffer.length >= this.PACKETS_PER_CHUNK) {
            this._dispatchPacketChunk();
        }
    }
}

_dispatchPacketChunk() {
    if (this._packetBuffer.length === 0) return;
    
    try {
        // Combine packets into a single chunk
        const totalLength = this._packetBuffer.reduce((sum, packet) => sum + packet.length, 0);
        const chunk = new Uint8Array(totalLength);
        
        let offset = 0;
        this._packetBuffer.forEach(packet => {
            chunk.set(packet, offset);
            offset += packet.length;
        });
        
        // Call the callback only if we have a valid chunk and we're not aborting
        if (totalLength > 0 && !this._requestAbort && this._onDataArrival) {
            try {
                this._onDataArrival(chunk.buffer, 0, totalLength);
            } catch (e) {
                Log.e(this.TAG, `Error in _onDataArrival callback: ${e.message}`);
            }
        }
    } catch (e) {
        Log.e(this.TAG, `Error in _dispatchPacketChunk: ${e.message}`);
    } finally {
        // Always clear the buffer, even if we encountered an error
        this._packetBuffer = [];
        this._lastFlushTime = Date.now();
    }
}

_findAlignedSyncByte(buffer) {
    // Look for a pattern of sync bytes at 188-byte intervals
    for (let i = 0; i <= buffer.length - 188*2; i++) {
        if (buffer[i] === 0x47) {
            // Check if we have multiple sync bytes at the expected interval
            let validSyncCount = 1;
            for (let j = 1; j <= 3; j++) {
                const nextSyncPos = i + (j * 188);
                if (nextSyncPos < buffer.length && buffer[nextSyncPos] === 0x47) {
                    validSyncCount++;
                } else {
                    break;
                }
            }

            // If we found at least 2 consecutive sync bytes at 188-byte intervals
            if (validSyncCount >= 2) {
                return i;
            }
        }
    }

    // Fallback: return the first sync byte we find
    for (let i = 0; i < buffer.length; i++) {
        if (buffer[i] === 0x47) {
            return i;
        }
    }

    return -1;
}

async _readChunks() {
    try {
        let pendingData = new Uint8Array(0);
        let readCount = 0;
        
        // Read as much as possible before the transport closes
        while (!this._requestAbort) {
            // Check transport validity at the beginning of each iteration
            if (!this._transport || this._transport.closed) {
                Log.w(this.TAG, "Transport is closed or null, exiting read loop");
                break;
            }
            
            // Check reader validity
            if (!this._reader) {
                Log.e(this.TAG, "Reader is null, cannot continue");
                break;
            }
            
            try {
                // Read with timeout protection
                const readPromise = this._reader.read();
                const timeoutPromise = new Promise((_, reject) => 
                    setTimeout(() => reject(new Error("Read timeout")), 5000)
                );
                
                const { value, done } = await Promise.race([readPromise, timeoutPromise]);
                
                if (done) {
                    Log.v(this.TAG, "Stream ended");
                    break;
                }
                
                if (value) {
                    readCount++;
                    let chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
                    
                    // Update last packet time
                    this.lastPacketTime = Date.now();
                    
                    // Combine with pending data if needed
                    if (pendingData.length > 0) {
                        const newBuffer = new Uint8Array(pendingData.length + chunk.length);
                        newBuffer.set(pendingData);
                        newBuffer.set(chunk, pendingData.length);
                        chunk = newBuffer;
                        pendingData = new Uint8Array(0);
                    }
                    
                    // Find sync byte and align
                    const syncIndex = this._findAlignedSyncByte(chunk);
                    
                    if (syncIndex === -1) {
                        pendingData = chunk;
                        continue;
                    } else if (syncIndex > 0) {
                        chunk = chunk.slice(syncIndex);
                    }
                    
                    // Process only complete 188-byte packets
                    const validLength = Math.floor(chunk.length / 188) * 188;
                    
                    if (validLength > 0) {
                        const validChunk = chunk.slice(0, validLength);
                        const packets = this.tsBuffer.addChunk(validChunk);
                        
                        if (packets) {
                            this._processPackets(packets);
                            Log.v(this.TAG, `Processed ${packets.length} MPEG-TS packets`);
                        }
                    }
                    
                    // Store remainder for next iteration
                    if (validLength < chunk.length) {
                        pendingData = chunk.slice(validLength);
                    }
                }
            } catch (e) {
                if (this._requestAbort) break;
                
                Log.e(this.TAG, `Error reading from stream: ${e.message}`);
                
                // The transport might have closed during read
                if (!this._transport || this._transport.closed) {
                    Log.w(this.TAG, "Transport closed during read operation");
                    break;
                }
                
                // For other errors, try to continue
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
        
        // Flush any remaining packets
        if (this._packetBuffer.length > 0 && !this._requestAbort) {
            this._dispatchPacketChunk();
        }
        
        Log.v(this.TAG, `Read loop exited after reading ${readCount} chunks`);
        
        // Cleanup
        this._cleanup();
        
        // Report error if not requested abort
        if (!this._requestAbort) {
            Log.e(this.TAG, "Stream reading terminated unexpectedly");
            this._status = LoaderStatus.kError;
            if (this._onError) {
                this._onError(LoaderErrors.CONNECTION_ERROR, {
                    code: -1,
                    msg: "Stream reading terminated unexpectedly"
                });
            }
        }
    } catch (e) {
        if (!this._requestAbort) {
            Log.e(this.TAG, `Fatal error in _readChunks: ${e.message}`);
            this._status = LoaderStatus.kError;
            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
            }
        }
    }
}

_startHeartbeat() {
    if (this._heartbeatInterval) {
        clearInterval(this._heartbeatInterval);
        this._heartbeatInterval = null;
    }
    
    // Set a very simple health check
    this._heartbeatInterval = setInterval(() => {
        if (!this._transport || this._transport.closed) {
            Log.v(this.TAG, "Transport no longer valid in heartbeat check");
        }
    }, 10000);
}

// New method to process a single stream to completion
async _processCurrentStream() {
    let pendingData = new Uint8Array(0);
    let streamError = null;
    
    try {
        // Process this stream until it ends or an error occurs
        while (!this._requestAbort) {
            try {
                // Validate transport is still valid
                if (!this._transport || this._transport.closed) {
                    throw new Error("Transport invalid during stream processing");
                }
                
                // Read from the current stream
                const { value, done } = await this._reader.read();
                
                // Check abort flag after the async read operation
                if (this._requestAbort) break;
                
                if (done) {
                    Log.v(this.TAG, `Stream read complete after ${this._receivedLength} bytes`);
                    
                    // Process any remaining valid data before exiting
                    if (pendingData.length >= 188) {
                        const validPacketsLength = Math.floor(pendingData.length / 188) * 188;
                        const packets = this.tsBuffer.addChunk(pendingData.slice(0, validPacketsLength));
                        if (packets) {
                            this._processPackets(packets);
                        }
                    }
                    
                    // Exit this stream's processing loop
                    return;
                }
                
                if (value) {
                    // Convert to Uint8Array if needed
                    let chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
                    
                    // Combine with any pending data from previous chunks
                    if (pendingData.length > 0) {
                        const newBuffer = new Uint8Array(pendingData.length + chunk.length);
                        newBuffer.set(pendingData, 0);
                        newBuffer.set(chunk, pendingData.length);
                        chunk = newBuffer;
                        pendingData = new Uint8Array(0);
                    }
                    
                    // Find and align to the first valid sync byte pattern
                    const syncIndex = this._findAlignedSyncByte(chunk);
                    
                    if (syncIndex === -1) {
                        // No valid pattern found, store the chunk for next iteration
                        pendingData = chunk;
                        Log.d(this.TAG, `No valid sync pattern found in chunk (${chunk.length} bytes)`);
                        continue;
                    } else if (syncIndex > 0) {
                        // Skip data before the first valid sync byte
                        chunk = chunk.slice(syncIndex);
                        Log.d(this.TAG, `Aligned to sync byte at offset ${syncIndex}`);
                    }
                    
                    // Process only complete 188-byte packets
                    const validLength = Math.floor(chunk.length / 188) * 188;
                    
                    if (validLength > 0) {
                        const validChunk = chunk.slice(0, validLength);
                        const packets = this.tsBuffer.addChunk(validChunk);
                        
                        if (packets) {
                            this._processPackets(packets);
                            // Update last packet time
                            this.lastPacketTime = Date.now();
                        } else {
                            Log.w(this.TAG, `Failed to extract valid packets from ${validLength} bytes`);
                        }
                    }
                    
                    // Store any remaining partial packet for the next chunk
                    if (validLength < chunk.length) {
                        pendingData = chunk.slice(validLength);
                    }
                }
            } catch (readError) {
                // Check if the operation was aborted
                if (this._requestAbort) {
                    Log.v(this.TAG, "Stream processing aborted as requested");
                    break;
                }
                
                Log.e(this.TAG, `Error reading from stream: ${readError.message}`);
                
                // If transport is closed, exit this stream's processing
                if (!this._transport || this._transport.closed) {
                    throw readError; // Let the outer loop handle reconnection
                }
                
                // For other errors, attempt to recover the current reader
                if (this._reader) {
                    try {
                        // Make a clean break by releasing the reader
                        this._reader.releaseLock();
                    } catch (releaseError) {
                        // Just log and continue if we can't release
                        Log.w(this.TAG, `Error releasing reader: ${releaseError.message}`);
                    }
                    this._reader = null;
                }
                
                // Exit current stream processing - the main loop will get a new stream
                streamError = readError;
                break;
            }
        }
    } catch (e) {
        Log.e(this.TAG, `Stream processing error: ${e.message}`);
        // Propagate the error to the main loop
        throw e;
    } finally {
        // Always ensure reader is properly released before returning
        if (this._reader) {
            try {
                // Attempt to cleanly cancel the reader if we still have one
                await this._reader.cancel("Stream processing complete").catch(e => {
                    Log.w(this.TAG, `Error canceling reader: ${e.message}`);
                });
            } catch (e) {
                Log.w(this.TAG, `Error during reader cleanup: ${e.message}`);
            }
        }
    }
    
    // If we had an error during processing, re-throw it
    if (streamError && !this._requestAbort) {
        throw streamError;
    }
}

async _processCurrentStream() {
    let pendingData = new Uint8Array(0);
    
    while (!this._requestAbort) {
        try {
            const { value, done } = await this._reader.read();
            
            if (done) {
                // Process any remaining data and exit this stream's processing
                // ...process pendingData...
                return;
            }
            
            if (value) {
                // Process the chunk as before
                // ...
            }
        } catch (e) {
            throw e; // Bubble up to be handled by _readChunks
        }
    }
}

async _handleTransportClosure(reason) {
    Log.w(this.TAG, `Transport closed: ${reason}`);
    
    if (this._requestAbort) return false;
    
    // Clear current state
    this._reader = null;
    this._transport = null;
    this._connectionEstablished = false;
    
    // Try to reconnect
    return await this._reconnectTransport();
}

async _reconnectTransport() {
    try {
        if (!this._dataSource || !this._dataSource.url) {
            Log.e(this.TAG, "Cannot reconnect: No data source URL available");
            return false;
        }
        
        // Max reconnection attempts
        const MAX_RECONNECT_ATTEMPTS = 3;
        let reconnectAttempt = 0;
        
        // Close and nullify any existing transport
        if (this._transport) {
            try {
                if (!this._transport.closed) {
                    await this._transport.close();
                }
            } catch (e) {
                // Just log errors
                Log.w(this.TAG, `Error closing existing transport: ${e.message}`);
            } finally {
                this._transport = null;
                this._connectionEstablished = false;
            }
        }
        
        // Nullify any existing reader
        this._reader = null;
        
        while (reconnectAttempt < MAX_RECONNECT_ATTEMPTS) {
            if (this._requestAbort) {
                return false;
            }
            
            reconnectAttempt++;
            
            // Calculate delay with exponential backoff and jitter
            const baseDelay = Math.min(1000 * Math.pow(2, reconnectAttempt - 1), 10000);
            const jitter = Math.random() * 1000;
            const delay = baseDelay + jitter;
            
            Log.v(this.TAG, `Waiting ${Math.round(delay)}ms before reconnection attempt ${reconnectAttempt}/${MAX_RECONNECT_ATTEMPTS}`);
            
            // Wait before attempting reconnection
            await new Promise(resolve => setTimeout(resolve, delay));
            
            if (this._requestAbort) {
                return false;
            }
            
            Log.v(this.TAG, `Attempting reconnection (attempt ${reconnectAttempt}/${MAX_RECONNECT_ATTEMPTS})`);
            
            try {
                // Create a new WebTransport connection with timeout
                this._transport = new WebTransport(this._dataSource.url);
                
                // Set up a timeout for the ready promise
                const readyPromise = this._transport.ready;
                const timeoutPromise = new Promise((_, reject) => {
                    setTimeout(() => reject(new Error("Connection timeout")), 10000);
                });
                
                // Wait for either ready or timeout
                await Promise.race([readyPromise, timeoutPromise]);
                
                // Check if the connection is still valid
                if (this._transport.closed) {
                    throw new Error("Transport closed immediately after connecting");
                }
                
                this._connectionEstablished = true;
                
                // Set up closed event handlers
                this._transport.closed
                    .then(info => this._onConnectionClosed(info))
                    .catch(error => this._onConnectionError(error));
                
                Log.v(this.TAG, "WebTransport reconnection successful");
                
                // Try to get the first stream to verify connection is fully working
                const incomingStreams = this._transport.incomingUnidirectionalStreams;
                const streamReader = incomingStreams.getReader();
                
                // Set up a timeout for reading the first stream
                const readStreamPromise = streamReader.read();
                const streamTimeoutPromise = new Promise((_, reject) => {
                    setTimeout(() => reject(new Error("Stream read timeout")), 5000);
                });
                
                // Wait for either stream or timeout
                const { value: stream, done } = await Promise.race([readStreamPromise, streamTimeoutPromise]);
                
                // Always release the reader
                try {
                    streamReader.releaseLock();
                } catch (e) {
                    Log.w(this.TAG, `Error releasing stream reader: ${e.message}`);
                }
                
                if (done || !stream) {
                    throw new Error("No stream available on reconnected transport");
                }
                
                // Got a valid stream, set up reader
                this._reader = stream.getReader();
                
                // Start heartbeat
                this._startHeartbeat();
                
                // Resume reading chunks
                this._readChunks();
                
                return true;
            } catch (e) {
                Log.e(this.TAG, `Reconnection attempt ${reconnectAttempt} failed: ${e.message}`);
                
                // Clean up failed transport
                if (this._transport) {
                    try {
                        if (!this._transport.closed) {
                            await this._transport.close();
                        }
                    } catch (closeError) {
                        // Just log errors
                    } finally {
                        this._transport = null;
                        this._connectionEstablished = false;
                    }
                }
            }
        }
        
        Log.e(this.TAG, `Failed to reconnect after ${MAX_RECONNECT_ATTEMPTS} attempts`);
        
        // Report error
        this._status = LoaderStatus.kError;
        if (this._onError) {
            this._onError(LoaderErrors.CONNECTION_ERROR, {
                code: -1,
                msg: `Failed to reconnect after ${MAX_RECONNECT_ATTEMPTS} attempts`
            });
        }
        
        return false;
    } catch (e) {
        Log.e(this.TAG, `Error in reconnection process: ${e.message}`);
        return false;
    }
}

async _reconnectTransport() {
    // Update state
    this._connectionState = 'reconnecting';
    
    try {
        if (!this._dataSource || !this._dataSource.url) {
            Log.e(this.TAG, "Cannot reconnect: No data source URL available");
            this._connectionState = 'failed';
            return false;
        }
        
        // Max reconnection attempts
        const MAX_RECONNECT_ATTEMPTS = 3;
        let reconnectAttempt = 0;
        let reconnectDelay = 1000; // Start with 1 second delay
        
        // CRITICAL: Reset reader state completely
        if (this._reader) {
            this._readerState = 'releasing';
            try {
                await this._reader.cancel("Preparing for reconnection");
            } catch (e) {
                // Just log errors
                Log.w(this.TAG, `Error canceling reader: ${e.message}`);
            } finally {
                this._reader = null;
                this._readerState = 'none';
            }
        }
        
        while (reconnectAttempt < MAX_RECONNECT_ATTEMPTS) {
            if (this._requestAbort) {
                Log.v(this.TAG, "Reconnection aborted by request");
                this._connectionState = 'failed';
                return false;
            }
            
            reconnectAttempt++;
            Log.v(this.TAG, `Attempting to reconnect transport (attempt ${reconnectAttempt}/${MAX_RECONNECT_ATTEMPTS})`);
            
            try {
                // Close old transport if present
                if (this._transport && !this._transport.closed) {
                    try {
                        await this._transport.close();
                    } catch (closeError) {
                        Log.w(this.TAG, `Error closing old transport: ${closeError.message}`);
                    } finally {
                        // Always null out the transport regardless of close result
                        this._transport = null;
                    }
                } else {
                    // Make sure to null out even if it was closed
                    this._transport = null;
                }
                
                this._connectionEstablished = false;
                
                // Add delay before creating new transport to allow cleanup
                await new Promise(resolve => setTimeout(resolve, 100));
                
                // Create a new WebTransport connection
                Log.v(this.TAG, "Creating new WebTransport instance");
                this._transport = new WebTransport(this._dataSource.url);
                
                // Wait for the connection to be ready with a timeout
                const readyPromise = this._transport.ready;
                
                // Create a timeout promise
                const timeoutPromise = new Promise((_, reject) => {
                    setTimeout(() => reject(new Error("Connection ready timeout")), 5000);
                });
                
                // Wait for ready or timeout
                await Promise.race([readyPromise, timeoutPromise]);
                
                // IMPORTANT: Verify the connection is still valid after waiting
                if (!this._transport || this._transport.closed) {
                    Log.w(this.TAG, "Transport was closed while waiting for ready state");
                    throw new Error("Transport closed during initialization");
                }
                
                this._connectionEstablished = true;
                
                // Set up connection event handlers
                this._transport.closed
                    .then(info => {
                        // IMPORTANT: Update state when connection closes
                        Log.w(this.TAG, `WebTransport connection closed with code: ${info.closeCode}`);
                        this._connectionState = 'idle';
                        this._onConnectionClosed(info);
                    })
                    .catch(error => {
                        // IMPORTANT: Update state on connection error
                        Log.e(this.TAG, `WebTransport connection error: ${error.message}`);
                        this._connectionState = 'failed';
                        this._onConnectionError(error);
                    });
                
                Log.v(this.TAG, "WebTransport reconnection successful");
                
                // Update state
                this._connectionState = 'connected';
                
                // IMPORTANT: Add a short delay and do a final check before returning
                await new Promise(resolve => setTimeout(resolve, 200));
                
                if (!this._transport || this._transport.closed) {
                    Log.w(this.TAG, "Transport became invalid after reconnection");
                    this._connectionState = 'idle';
                    throw new Error("Transport invalid after reconnection");
                }
                
                // Restart the heartbeat mechanism
                this._startHeartbeat();
                
                return true;
            } catch (e) {
                Log.e(this.TAG, `Reconnection attempt ${reconnectAttempt} failed: ${e.message}`);
                
                // Clean up any partially created transport
                if (this._transport && !this._transport.closed) {
                    try {
                        await this._transport.close();
                    } catch (closeError) {
                        // Just log
                    }
                }
                this._transport = null;
                this._connectionEstablished = false;
                
                // Wait before retrying, with exponential backoff
                await new Promise(resolve => setTimeout(resolve, reconnectDelay));
                reconnectDelay = Math.min(reconnectDelay * 2, 5000); // Maximum 5 second delay
            }
        }
        
        // If we reach here, all reconnection attempts failed
        Log.e(this.TAG, `Failed to reconnect after ${MAX_RECONNECT_ATTEMPTS} attempts`);
        
        // Report the error since we failed to reconnect
        this._status = LoaderStatus.kError;
        if (this._onError) {
            this._onError(LoaderErrors.CONNECTION_ERROR, {
                code: -1,
                msg: `Failed to reconnect after ${MAX_RECONNECT_ATTEMPTS} attempts`
            });
        }
        
        this._connectionState = 'failed';
        return false;
    } catch (e) {
        Log.e(this.TAG, `Error in reconnection process: ${e.message}`);
        
        // Clean up any partially created transport
        if (this._transport && !this._transport.closed) {
            try {
                await this._transport.close();
            } catch (closeError) {
                // Just log
            }
        }
        this._transport = null;
        this._connectionEstablished = false;
        
        this._connectionState = 'failed';
        return false;
    }
}

async _safeAcquireStream() {
    // Validate state
    if (this._readerState !== 'none') {
        Log.w(this.TAG, `Cannot acquire stream, reader state is ${this._readerState}`);
        return false;
    }
    
    if (this._connectionState !== 'connected') {
        Log.w(this.TAG, `Cannot acquire stream, connection state is ${this._connectionState}`);
        return false;
    }
    
    // Mark that we're acquiring
    this._readerState = 'acquiring';
    
    try {
        // IMPORTANT: Add more thorough validation of transport
        if (!this._transport) {
            Log.e(this.TAG, "Transport is null when trying to acquire stream");
            this._readerState = 'none';
            this._connectionState = 'idle'; // Reset connection state
            return false;
        }
        
        if (this._transport.closed) {
            Log.e(this.TAG, "Transport is closed when trying to acquire stream");
            this._readerState = 'none';
            this._connectionState = 'idle'; // Reset connection state
            return false;
        }
        
        // IMPORTANT: Wait a small amount of time to ensure connection is stable
        await new Promise(resolve => setTimeout(resolve, 50));
        
        // Check again after the short delay
        if (!this._transport || this._transport.closed) {
            Log.e(this.TAG, "Transport became invalid during acquisition delay");
            this._readerState = 'none';
            this._connectionState = 'idle';
            return false;
        }
        
        // Get streams safely with timeout protection
        let streamReaderAcquired = false;
        let streamReader = null;
        
        try {
            Log.v(this.TAG, "Getting incomingUnidirectionalStreams");
            const incomingStreams = this._transport.incomingUnidirectionalStreams;
            
            Log.v(this.TAG, "Acquiring stream reader");
            streamReader = incomingStreams.getReader();
            streamReaderAcquired = true;
            
            Log.v(this.TAG, "Reading from stream reader");
            const { value: stream, done } = await streamReader.read();
            
            // Always release the reader lock
            if (streamReaderAcquired) {
                try {
                    Log.v(this.TAG, "Releasing stream reader lock");
                    streamReader.releaseLock();
                    streamReaderAcquired = false;
                } catch (releaseError) {
                    Log.w(this.TAG, `Error releasing stream reader lock: ${releaseError.message}`);
                }
            }
            
            if (done || !stream) {
                Log.w(this.TAG, "No stream available");
                this._readerState = 'none';
                return false;
            }
            
            // Now we have a stream, get its reader
            Log.v(this.TAG, "Creating reader for received stream");
            this._reader = stream.getReader();
            this._readerState = 'active';
            
            const streamId = `stream-${this._streamIdCounter++}`;
            Log.v(this.TAG, `New stream acquired: ${streamId}`);
            
            if (this.streamRotationHandler) {
                this.streamRotationHandler.registerStream(streamId);
            }
            
            return true;
        } catch (e) {
            // Make sure to release the lock on error
            if (streamReaderAcquired && streamReader) {
                try {
                    Log.v(this.TAG, "Releasing stream reader lock after error");
                    streamReader.releaseLock();
                } catch (releaseError) {
                    // Just log
                    Log.w(this.TAG, `Error releasing stream reader after error: ${releaseError.message}`);
                }
            }
            
            throw e; // Re-throw to be caught by outer handler
        }
    } catch (e) {
        Log.e(this.TAG, `Error acquiring stream: ${e.message}`);
        this._readerState = 'none';
        // Check if the error indicates a bad connection
        if (e.message.includes("Transport") || e.message.includes("connection")) {
            this._connectionState = 'idle';
        }
        return false;
    }
}

async _getNextStream() {
    try {
        if (!this._transport || this._transport.closed) {
            Log.e(this.TAG, "Transport is closed or null, cannot get next stream");
            return false;
        }

        // IMPORTANT: Make sure to release the old reader first
        if (this._reader) {
            try {
                await this._reader.cancel("Getting new stream");
            } catch (e) {
                Log.w(this.TAG, `Error canceling previous reader: ${e.message}`);
            }
            this._reader = null;
        }

        Log.v(this.TAG, "Attempting to get next stream");

        try {
            // Get the incoming streams reader
            const incomingStreams = this._transport.incomingUnidirectionalStreams;
            const streamReader = incomingStreams.getReader();

            // Try to read the next stream
            const { value: stream, done } = await streamReader.read();

            // CRITICAL: Always release the reader lock before returning
            try {
                streamReader.releaseLock();
            } catch (e) {
                Log.w(this.TAG, `Error releasing stream reader lock: ${e.message}`);
            }

            if (done) {
                Log.v(this.TAG, "No more streams available");
                return false;
            }

            if (!stream) {
                Log.w(this.TAG, "Received null stream");
                return false;
            }

            // Got a new stream, create reader
            this._reader = stream.getReader();

            // Update statistics
            const streamId = `stream-${this._streamIdCounter++}`;
            Log.v(this.TAG, `New stream received: ${streamId}`);

            if (this.streamRotationHandler) {
                this.streamRotationHandler.registerStream(streamId);
            }

            if (this.testHarness && typeof this.testHarness.recordStreamSwitch === 'function') {
                this.testHarness.recordStreamSwitch();
            }

            return true;
        } catch (streamError) {
            Log.e(this.TAG, `Error getting stream: ${streamError.message}`);
            return false;
        }
    } catch (e) {
        Log.e(this.TAG, `Error in _getNextStream: ${e.message}`);
        return false;
    }
}

async _monitorIncomingStreams() {
    try {
        Log.v(this.TAG, "Starting to monitor incoming streams");
        
        if (!this._transport) {
            Log.e(this.TAG, "Transport not available for stream monitoring");
            return;
        }
        
        const incomingStreams = this._transport.incomingUnidirectionalStreams;
        const streamReader = incomingStreams.getReader();
        
        while (!this._requestAbort) {
            try {
                if (!this._transport || this._transport.closed) {
                    Log.e(this.TAG, "Transport closed during stream monitoring");
                    break;
                }
                
                const { value: stream, done } = await streamReader.read();
                
                if (done) {
                    Log.v(this.TAG, "No more incoming streams available");
                    break;
                }
                
                if (!stream) {
                    Log.w(this.TAG, "Received null stream, continuing");
                    continue;
                }
                
                const streamId = `stream-${this._streamIdCounter++}`;
                Log.v(this.TAG, `New stream received: ${streamId}`);
                
                const reader = stream.getReader();
                
                if (this.config.enableStreamRotationHandler) {
                    this.streamRotationHandler.registerStream(streamId);
                    
                    if (this.testHarness && typeof this.testHarness.recordStreamSwitch === 'function') {
                        this.testHarness.recordStreamSwitch();
                    }
                }
                
                this._activeStreams.set(streamId, {
                    reader: reader,
                    startTime: Date.now(),
                    status: 'active',
                    bytesReceived: 0,
                    packetsProcessed: 0,
                    error: null
                });
                
                this._processStreamData(streamId, reader);
            } catch (e) {
                if (this._requestAbort) break;
                
                Log.e(this.TAG, `Error in stream monitoring: ${e.message}`);
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    } catch (e) {
        if (!this._requestAbort) {
            Log.e(this.TAG, `Stream monitor error: ${e.message}`);
        }
    }
}

advancedStreamDiagnosis() {
    // Skip if already shut down
    if (this._requestAbort) return;
    
    try {
        Log.v(this.TAG, "========= MPEG-TS STREAM DIAGNOSTIC REPORT =========");
        
        // Connection status
        const transportState = this._transport ?
            (this._transport.closed ? 'closed' : 'open') : 'null';
        Log.v(this.TAG, `WebTransport State: ${transportState}`);
        
        if (this._transport) {
            Log.v(this.TAG, `Ready Promise: ${this._transport.ready ? 'resolved' : 'pending'}`);
        }
        
        Log.v(this.TAG, `Connection Established: ${this._connectionEstablished ? 'YES' : 'no'}`);
        Log.v(this.TAG, `Current Reader: ${this._reader ? 'active' : 'none'}`);
        Log.v(this.TAG, `Total Bytes Received: ${this._receivedLength}`);
        
        // Stream rotation status
        if (this.config.enableStreamRotationHandler && this.streamRotationHandler) {
            const stats = this.streamRotationHandler.getStats();
            Log.v(this.TAG, "Stream Rotation Stats:");
            Log.v(this.TAG, `  Active Streams: ${stats.activeStreams}`);
            Log.v(this.TAG, `  Total Streams Received: ${stats.streamsReceived}`);
            Log.v(this.TAG, `  Streams Switched: ${stats.streamsSwitched}`);
            Log.v(this.TAG, `  PTS Jumps: ${stats.ptsJumps}`);
            Log.v(this.TAG, `  Stream Overlaps: ${stats.streamOverlaps}`);
            Log.v(this.TAG, `  In Transition: ${stats.inTransition ? 'YES' : 'no'}`);
            Log.v(this.TAG, `  Buffered Packets: ${stats.bufferedPackets}`);
        }
        
        // Active streams info
        Log.v(this.TAG, `Active Streams: ${this._activeStreams.size}`);
        for (const [streamId, stream] of this._activeStreams.entries()) {
            Log.v(this.TAG, `  Stream ${streamId}: ${stream.status}`);
            Log.v(this.TAG, `    Started: ${new Date(stream.startTime).toISOString()}`);
            Log.v(this.TAG, `    Bytes Received: ${stream.bytesReceived}`);
            Log.v(this.TAG, `    Packets Processed: ${stream.packetsProcessed}`);
            if (stream.error) {
                Log.v(this.TAG, `    Error: ${stream.error}`);
            }
        }
        
        // Buffer statistics
        if (this.tsBuffer) {
            Log.v(this.TAG, `TS Buffer statistics:`);
            Log.v(this.TAG, `  Total packets processed: ${this.tsBuffer.stats.totalPacketsProcessed}`);
            Log.v(this.TAG, `  Valid packets: ${this.tsBuffer.stats.validPackets}`);
            Log.v(this.TAG, `  Invalid packets: ${this.tsBuffer.stats.invalidPackets}`);
            Log.v(this.TAG, `  Invalid sync bytes: ${this.tsBuffer.stats.invalidSyncByteCount}`);
        }
        
        // PTS statistics
        if (this.packetLogger) {
            Log.v(this.TAG, "PTS/timing statistics:");
            Log.v(this.TAG, `  Video packets: ${this.packetLogger.debugStats.videoPIDPackets}`);
            Log.v(this.TAG, `  PES packet starts: ${this.packetLogger.debugStats.pesStarts}`);
            Log.v(this.TAG, `  Valid PTS found: ${this.packetLogger.debugStats.validPTS}`);
            Log.v(this.TAG, `  Last PTS value: ${this.packetLogger.lastValidPTS}`);
        }
        
        // Output buffer status
        Log.v(this.TAG, `Packet buffer: ${this._packetBuffer.length} packets`);
        Log.v(this.TAG, `Output buffer: ${this._outputBuffer.length} packets`);
        
        Log.v(this.TAG, "====================================================");
    } catch (e) {
        Log.e(this.TAG, `Error running diagnostics: ${e.message}`);
    }
}

async abort() {
    // First, clear any timers or intervals
    if (this.diagnosticTimer) {
        clearInterval(this.diagnosticTimer);
        this.diagnosticTimer = null;
    }

    // Clear heartbeat
    if (this._heartbeatInterval) {
        clearInterval(this._heartbeatInterval);
        this._heartbeatInterval = null;
    }
    
    // Signal abort request
    this._requestAbort = true;
    
    try {
        // Cancel all active stream readers
        for (const [streamId, streamData] of this._activeStreams.entries()) {
            if (streamData.reader) {
                try {
                    await streamData.reader.cancel("Loader aborted").catch(e => {
                        Log.w(this.TAG, `Error canceling reader for stream ${streamId}: ${e.message}`);
                    });
                } catch (e) {
                    // Ignore errors when canceling readers
                }
            }
        }
        
        // Clear active streams
        this._activeStreams.clear();
        this._reader = null;
        
        // Flush any pending data from both buffers
        if (this._packetBuffer.length > 0) {
            this._dispatchPacketChunk();
        }
        
        if (this._outputBuffer.length > 0) {
            this._flushOutputBuffer();
        }
        
        // Close the transport with a clean reason
        if (this._transport && !this._transport.closed) {
            Log.v(this.TAG, "Closing WebTransport connection...");
            await this._transport.close({closeCode: 0, reason: "Player destroyed"}).catch(e => {
                Log.w(this.TAG, `Error closing transport: ${e.message}`);
            });
        }
    } catch (e) {
        Log.e(this.TAG, `Error during abort: ${e.message}`);
    } finally {
        // Ensure nullification even if errors occur
        this._transport = null;
        this._packetBuffer = [];
        this._outputBuffer = [];
        this.tsBuffer = null;
        if (this.streamRotationHandler) {
            this.streamRotationHandler.reset();
        }
        this._status = LoaderStatus.kComplete;
        
        Log.v(this.TAG, "WebTransport connection cleanup complete");
    }
}

    getStreamHealthReport() {
        // Get rotation handler stats
        const rotationStats = this.streamRotationHandler ? 
            this.streamRotationHandler.getStats() : {};
            
        // Get active stream details
        const streamDetails = [];
        for (const [streamId, streamData] of this._activeStreams.entries()) {
            streamDetails.push({
                id: streamId,
                status: streamData.status,
                durationMs: Date.now() - streamData.startTime,
                bytesReceived: streamData.bytesReceived,
                packetsProcessed: streamData.packetsProcessed,
                error: streamData.error || null
            });
        }
            
        return {
            receivedBytes: this._receivedLength,
            packetStats: {
                total: this.tsBuffer.stats.totalPacketsProcessed,
                valid: this.tsBuffer.stats.validPackets,
                invalid: this.tsBuffer.stats.invalidPackets
            },
            ptsStats: {
                ptsFound: this.packetLogger.debugStats.validPTS,
                lastPTS: this.packetLogger.lastValidPTS
            },
            rotationStats: rotationStats,
            streamCount: this._activeStreams.size,
            streamDetails: streamDetails,
            outputBufferLength: this._outputBuffer.length,
            lastFlushTime: this._lastFlushTime
        };
    }
    
    getStreamQualityReport() {
        return this.testHarness.generateReport();
    }
}

export default WebTransportLoader;
