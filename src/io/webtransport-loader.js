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
 * Main WebTransportLoader class - handles WebTransport streams with rotation support
 */
class WebTransportLoader extends BaseLoader {
    constructor(config = {}) {
        super('webtransport-loader');
        this.TAG = 'WebTransportLoader';

        // Basic loader properties
        this._needStash = true;
        this._transport = null;
        this._requestAbort = false;
        this._receivedLength = 0;
        this.debugCounter = 0;

        // Configure settings
        this.config = {
            enableStreamRotationHandler: config.enableStreamRotationHandler !== false,
            minBufferTime: config.minBufferTime || 1000,     // Min time to buffer before output
            maxBufferTime: config.maxBufferTime || 5000,     // Max time to buffer before forcing output
            packetFlushThreshold: config.packetFlushThreshold || 100,  // Number of packets per chunk
            maxConcurrentStreams: config.maxConcurrentStreams || 5,    // Max concurrent streams to handle
            enableDetailedLogging: config.enableDetailedLogging || false
        };

// Initialize logging function
        const logFunction = (msg) => this.config.enableDetailedLogging ? Log.v(this.TAG, msg) : null;
        
        // Initialize components
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
        
        // Bind methods
        this._monitorIncomingStreams = this._monitorIncomingStreams.bind(this);
        this._processStreamData = this._processStreamData.bind(this);
        this._flushOutputBuffer = this._flushOutputBuffer.bind(this);
    }

	static isSupported() {
	    try {
		if (typeof self.WebTransport === 'undefined') {
		    return false;
		}
		
		// Additional compatibility checks
		const userAgent = navigator.userAgent;
		if (userAgent.includes('Chrome/') || 
		    userAgent.includes('Edge/') || 
		    (userAgent.includes('Safari/') && !userAgent.includes('Chrome/'))) {
		    // Check version through regex
		    const versionMatch = /Chrome\/(\d+)|Version\/(\d+\.\d+)/.exec(userAgent);
		    if (versionMatch) {
			const version = versionMatch[1] || versionMatch[2];
			return parseFloat(version) >= 87;
		    }
		}
		
		// For other browsers, just check if WebTransport exists
		return true;
	    } catch (e) {
		return false;
	    }
	}

    destroy() {
        if (this._transport) {
            this.abort();
        }
        
        // Clean up all active streams
        for (const [streamId, streamData] of this._activeStreams.entries()) {
            if (streamData.reader) {
                try {
                    streamData.reader.cancel("Loader destroyed");
                } catch (e) {
                    // Ignore errors during cleanup
                }
            }
        }
        
        this._activeStreams.clear();
        this.streamRotationHandler = null;
        this.tsBuffer = null;
        this.packetLogger = null;
        this.packetValidator = null;
        
        super.destroy();
    }

async open(dataSource) {
    try {
        if (!dataSource.url.startsWith('https://')) {
            throw new Error('WebTransport requires HTTPS URL');
        }

        // Store URL for potential reconnection
        this._lastUrl = dataSource.url;
        this._startTime = Date.now();
        
        Log.v(this.TAG, `Opening WebTransport connection to ${dataSource.url}`);

        // Create a new WebTransport instance - ONLY ONE INSTANCE
        this._transport = new WebTransport(dataSource.url);
        
        // Set up error handler for connection
        this._transport.closed.catch(error => {
            Log.e(this.TAG, `WebTransport connection closed with error: ${error.message}`);
            this._handleConnectionFailure();
        });
        
        // Wait for connection to be ready
        await this._transport.ready;
        
        Log.v(this.TAG, "WebTransport connection established successfully");
        this._status = LoaderStatus.kBuffering;

        // Get incoming streams
        const incomingStreams = this._transport.incomingUnidirectionalStreams;
        const streamReader = incomingStreams.getReader();
        
        // Read the first stream
        const { value: stream, done } = await streamReader.read();
        
        if (done || !stream) {
            throw new Error('No incoming stream received');
        }
        
        // Set up our reader with the first stream
        this._reader = stream.getReader();
        
        // Set up test harness
        this.testHarness.reset();
        this.testHarness.integrateWithLoader(this);
        
        // Start reading data from the first stream
        this._readChunks();
        
        // Start monitoring for additional streams
        this._monitorIncomingStreams(streamReader);
        
        // Set up diagnostic timer
        this.diagnosticTimer = setInterval(() => {
            this.advancedStreamDiagnosis();
        }, 10000);
        
    } catch (e) {
        Log.e(this.TAG, `Error opening WebTransport connection: ${e.message}`);
        this._status = LoaderStatus.kError;
        if (this._onError) {
            this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
        }
    }
}

    /**
     * Monitor for incoming unidirectional streams from the server
     */
	async _monitorIncomingStreams(streamReader) {
	    if (!this._transport || this._transport.closed) {
		Log.e(this.TAG, "Cannot monitor streams: transport is closed or null");
		return;
	    }
	    
	    // We already have the streamReader as a parameter
	    // This should be the reader for incomingUnidirectionalStreams
	    
	    try {
		Log.v(this.TAG, "Monitoring for additional incoming streams");
		
		let streamCounter = 1; // First stream is already being processed
		
		while (!this._requestAbort) {
		    try {
			const { value: stream, done } = await streamReader.read();
			
			if (done) {
			    Log.v(this.TAG, "No more incoming streams available");
			    break;
			}
			
			if (!stream) {
			    Log.w(this.TAG, "Received null stream, continuing");
			    continue;
			}
			
			streamCounter++;
			const streamId = `stream-${streamCounter}`;
			Log.v(this.TAG, `New stream detected: ${streamId}, switching readers`);
			
			// Record the time of the stream switch for diagnostics
			this._lastStreamSwitchTime = Date.now();
			
			// Wait for current read loop to complete if there is one
			if (this._reader) {
			    try {
				// Signal that we're about to change readers
				this._streamSwitchPending = true;
				
				// Give time for current read loop to finish
				await new Promise(resolve => setTimeout(resolve, 50));
				
				// Cancel existing reader
				await this._reader.cancel("Switching to new stream");
			    } catch (e) {
				// Ignore errors when canceling the reader
			    }
			}
			
			// Update reader to new stream
			this._reader = stream.getReader();
			
			// Start reading from the new stream
			this._readChunks();
			
			// Clear stream switch flag
			this._streamSwitchPending = false;
		    } catch (streamError) {
			if (this._requestAbort) break;
			
			Log.e(this.TAG, `Error reading stream: ${streamError.message}`);
			// Continue trying to read more streams despite errors
			await new Promise(resolve => setTimeout(resolve, 500));
		    }
		}
	    } catch (e) {
		if (!this._requestAbort) {
		    Log.e(this.TAG, `Error monitoring streams: ${e.message}`);
		    if (this._onError) {
			this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
		    }
		}
	    }
	}


	async _readChunks() {
	    // If a read operation is already in progress, don't start another one
	    if (this._readInProgress) {
		return;
	    }

	    this._readInProgress = true;

	    try {
		let pendingData = new Uint8Array(0);

		while (!this._requestAbort && !this._streamSwitchPending) {
		    try {
			// Check if reader is available
			if (!this._reader) {
			    Log.e(this.TAG, "Reader is null, cannot read chunks");
			    break;
			}

			const { value, done } = await this._reader.read();

			// Check abort flag again after the async read operation
			if (this._requestAbort || this._streamSwitchPending) break;

			if (done) {
			    Log.v(this.TAG, `Stream read complete after ${this._receivedLength} bytes`);
			    // Process any remaining valid data
			    if (pendingData.length >= 188) {
				const validPacketsLength = Math.floor(pendingData.length / 188) * 188;
				const packets = this.tsBuffer.addChunk(pendingData.slice(0, validPacketsLength));
				if (packets) {
				    this._processPackets(packets);
				}
			    }
			    break;
			}

			if (value) {
			    // Ensure we're working with Uint8Array
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
				continue;
			    } else if (syncIndex > 0) {
				// Skip data before the first valid sync byte
				chunk = chunk.slice(syncIndex);
			    }

			    // Process only complete 188-byte packets
			    const validLength = Math.floor(chunk.length / 188) * 188;

			    if (validLength > 0) {
				const validChunk = chunk.slice(0, validLength);
				const packets = this.tsBuffer.addChunk(validChunk);

				if (packets) {
				    this._processPackets(packets);
				}
			    }

			    // Store any remaining partial packet for the next chunk
			    if (validLength < chunk.length) {
				pendingData = chunk.slice(validLength);
			    }
			}
		    } catch (readError) {
			// If the error is due to abort or stream switch, handle gracefully
			if (this._requestAbort || this._streamSwitchPending) {
			    break;
			}

			// Check if the connection is closed
			if (this._transport && this._transport.closed) {
			    Log.e(this.TAG, "WebTransport connection is closed, cannot continue reading");
			    break;
			}

			// Log the error but continue reading if possible
			Log.e(this.TAG, `Error reading chunk: ${readError.message}`);

			// Small delay before retry
			await new Promise(resolve => setTimeout(resolve, 100));
		    }
		}

		// Flush any remaining packets if we're not aborting
		if (this._packetBuffer.length > 0 && !this._requestAbort) {
		    this._dispatchPacketChunk();
		}

	    } catch (e) {
		Log.e(this.TAG, `Error in _readChunks: ${e.message}`);
		if (e.stack) {
		    Log.e(this.TAG, e.stack);
		}

		if (!this._requestAbort) {
		    this._status = LoaderStatus.kError;
		    if (this._onError) {
			this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
		    }
		}
	    } finally {
		this._readInProgress = false;

		// If we're switching streams, signal that we've finished
		if (this._streamSwitchPending) {
		    Log.v(this.TAG, "Read operation terminated due to stream switch");
		}
	    }
	}

	    /**
	     * Process data from a single stream
	     * @param {string} streamId - The ID of the stream to process
	     */

	// Note: This method is no longer needed and should be removed.
	// The functionality has been merged into _readChunks and _monitorIncomingStreams

	// If you need to keep it for compatibility with other code, here's a simplified version:

	async _processStreamData(stream, streamId) {
	    Log.v(this.TAG, `Processing stream ${streamId} - deprecated method`);

	    // In the new implementation, we don't use this method anymore
	    // Stream data is directly processed in _readChunks

	    const reader = stream.getReader();

	    try {
		while (!this._requestAbort) {
		    const { value, done } = await reader.read();

		    if (done) {
			Log.v(this.TAG, `Stream ${streamId} ended`);
			break;
		    }

		    if (value) {
			Log.v(this.TAG, `Received ${value.byteLength} bytes from stream ${streamId}`);
			// We don't process the data here, just log it since this method is deprecated
		    }
		}
	    } catch (e) {
		Log.e(this.TAG, `Error reading stream ${streamId}: ${e.message}`);
	    } finally {
		try {
		    reader.releaseLock();
		} catch (e) {
		    // Ignore errors during cleanup
		}
	    }

	    Log.v(this.TAG, `Stream ${streamId} processing completed`);
	}

	    /**
	     * Handle processed packets from the rotation handler
	     * @param {Array} packets - Array of processed TS packets
	     */
	    _handleProcessedPackets(packets) {
		if (!packets || packets.length === 0) {
		    return;
		}
		
		// Add packets to our output buffer
		this._outputBuffer.push(...packets);
		
		// Check if we have enough packets to dispatch
		if (this._outputBuffer.length >= this.PACKETS_PER_CHUNK) {
		    this._flushOutputBuffer();
		}
	    }
	    
	    /**
	     * Flush the output buffer and dispatch to the demuxer
	     * @returns {boolean} - True if data was flushed, false otherwise
	     */
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
		
		// Log packets for diagnostics if enabled
		if (this.config.enableDetailedLogging) {
		    packetsToDispatch.forEach(packet => {
			this.packetLogger.logPacket(packet, Date.now());
		    });
		}
		
		// Calculate byte positions for data arrival
		const byteStart = this._receivedLength - totalLength;
		
		// Update the last flush time
		this._lastFlushTime = Date.now();
		
		// Determine if we're in a stream transition
		const inTransition = this.streamRotationHandler.isInTransition;
		
		// Create metadata about the chunk
		const metadata = {
		    isStreamTransition: inTransition,
		    transitionSequence: this.streamRotationHandler.transitionSequenceCounter,
		    ptsOffset: this.streamRotationHandler.globalPTSOffset,
		    activeStreams: this._activeStreams.size,
		    isInitialized: this._isInitialized === true
		};
		
		// Mark as initialized after first chunk
		if (!this._isInitialized) {
		    this._isInitialized = true;
		}
		
		// Send the data to the demuxer
		if (this._onDataArrival) {
		    this._onDataArrival(chunk.buffer, byteStart, this._receivedLength, metadata);
		}
		
		return true;
	    }
	    
	    /**
	     * Check and flush buffers if needed based on time or size thresholds
	     * @private
	     */
	    _checkAndFlushBuffers() {
		// If no new data has been received recently, there's nothing to flush
		if (this._outputBuffer.length === 0) {
		    return;
		}
		
		const now = Date.now();
		const timeSinceLastFlush = now - (this._lastFlushTime || 0);
		
		// Force a flush if we have data and it's been too long since the last flush
		// or if the buffer is getting too large
		if ((timeSinceLastFlush > this.config.maxBufferTime) ||
		    (this._outputBuffer.length >= this.PACKETS_PER_CHUNK * 2)) {
		    Log.v(this.TAG, `Forcing buffer flush after ${timeSinceLastFlush}ms with ${this._outputBuffer.length} packets`);
		    this._flushOutputBuffer();
		}
	    }
	    
	/**
	 * Run diagnostics and health checks
	 * @private
	 */
	_runDiagnostics() {
	    if (this._requestAbort) return;

	    try {
		// Log general statistics
		Log.v(this.TAG, "========= MPEG-TS STREAM DIAGNOSTIC REPORT =========");
		
		// WebTransport connection status
		if (this._transport) {
		    const state = this._transport.closed ? 'closed' : 'open';
		    Log.v(this.TAG, `WebTransport State: ${state}`);
		    Log.v(this.TAG, `WebTransport Ready Promise: ${this._transport.ready ? 'resolved' : 'pending'}`);
		    Log.v(this.TAG, `WebTransport Closed: ${this._transport.closed ? 'YES' : 'no'}`);
		    
		    // Check if streams are being received
		    Log.v(this.TAG, `Current Reader: ${this._reader ? 'active' : 'none'}`);
		    Log.v(this.TAG, `Read In Progress: ${this._readInProgress ? 'YES' : 'no'}`);
		    Log.v(this.TAG, `Stream Switch Pending: ${this._streamSwitchPending ? 'YES' : 'no'}`);
		} else {
		    Log.v(this.TAG, `WebTransport: null (connection not established)`);
		}
		
		Log.v(this.TAG, `Last Stream Switch: ${this._lastStreamSwitchTime ? new Date(this._lastStreamSwitchTime).toISOString().substring(11, 19) : 'never'}`);
		Log.v(this.TAG, `Total Bytes Received: ${this._receivedLength}`);
		
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
		    Log.v(this.TAG, `  Global PTS tracking: ${this._lastGlobalPTS || 'not initialized'}`);
		    Log.v(this.TAG, `  PTS offset: ${this._ptsOffset || 0}`);
		    
		    // Check if we're getting PTS values
		    if (this.packetLogger.debugStats.validPTS === 0 && this._receivedLength > 50000) {
			Log.w(this.TAG, "WARNING: No valid PTS values found despite receiving data");
		    }
		}

		// Output buffer status
		Log.v(this.TAG, `Current packet buffer: ${this._packetBuffer.length} packets`);
		
		// Check for no data received
		if (this._transport && !this._transport.closed && this._receivedLength === 0) {
		    const elapsedSecs = (Date.now() - this._startTime) / 1000;
		    if (elapsedSecs > 5) {
			Log.w(this.TAG, `WARNING: No data received after ${elapsedSecs.toFixed(1)} seconds with open connection`);
			
			// If we've been waiting too long with no data, try refreshing the reader
			if (elapsedSecs > 10 && !this._readInProgress && this._reader) {
			    Log.v(this.TAG, "Attempting to restart reader due to no data");
			    this._readChunks();
			}
		    }
		}

		Log.v(this.TAG, "====================================================");
	    } catch (e) {
		Log.e(this.TAG, `Error running diagnostics: ${e.message}`);
	    }
	}

	/**
	 * Check if the loader is in a stuck state and try to recover
	 * @private
	 */
	_checkForStuckState() {
	    const now = Date.now();
	    
	    // If we haven't received any data in 30 seconds but WebTransport appears open, we're stuck
	    if (this._transport && 
		!this._transport.closed && 
		this._receivedLength === 0 && 
		now - this._startTime > 30000) {
		
		Log.w(this.TAG, "Detected stuck state - no data received after 30 seconds");
		
		// Try to restart the connection
		Log.v(this.TAG, "Attempting recovery by restarting transport connection");
		
		// Store the current URL for reconnection
		const currentUrl = this._transport.url || this._lastUrl;
		
		// Close the current connection
		this._transport.close().catch(() => {});
		
		// Clear all streams
		this._activeStreams.clear();
		
		// Attempt reconnection after a short delay
		setTimeout(() => {
		    if (!this._requestAbort && currentUrl) {
			Log.v(this.TAG, `Reconnecting to ${currentUrl}`);
			this.open({ url: currentUrl });
		    }
		}, 2000);
	    }
	}

	    /**
	     * Abort and clean up the loader
	     */

	async abort() {
	    // First, clear any timers or intervals
	    if (this.diagnosticTimer) {
		clearInterval(this.diagnosticTimer);
		this.diagnosticTimer = null;
	    }

	    // Signal abort request to stop all operations
	    this._requestAbort = true;

	    try {
		// Cancel current reader if it exists
		if (this._reader) {
		    try {
			await this._reader.cancel("Loader aborted").catch(e => {
			    Log.w(this.TAG, `Error canceling reader: ${e.message}`);
			});
			this._reader = null;
		    } catch (e) {
			// Ignore errors during cleanup
			Log.w(this.TAG, `Error during reader cancel: ${e.message}`);
		    }
		}

		// Ensure any pending data is processed
		if (this._packetBuffer.length > 0) {
		    try {
			this._dispatchPacketChunk();
		    } catch (e) {
			Log.w(this.TAG, `Error dispatching final packet chunk: ${e.message}`);
		    }
		}

		// Close the transport with a clean reason
		if (this._transport && !this._transport.closed) {
		    Log.v(this.TAG, "Closing WebTransport connection...");
		    try {
			await this._transport.close({
			    closeCode: 0,
			    reason: "Player destroyed"
			}).catch(e => {
			    Log.w(this.TAG, `Error closing transport: ${e.message}`);
			});
		    } catch (e) {
			Log.w(this.TAG, `Exception during transport close: ${e.message}`);
		    }
		}
	    } catch (e) {
		Log.e(this.TAG, `Error during abort: ${e.message}`);
	    } finally {
		// Ensure nullification even if errors occur
		this._transport = null;
		this._reader = null;
		this._packetBuffer = [];
		this.tsBuffer = null;
		this._readInProgress = false;
		this._streamSwitchPending = false;
		this._status = LoaderStatus.kComplete;

		Log.v(this.TAG, "WebTransport connection cleanup complete");
	    }
	}

	    /**
	     * Get stream health report with detailed statistics
	     * @returns {Object} - Health report object
	     */
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
	}

	export default WebTransportLoader;
