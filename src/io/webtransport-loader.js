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

/**
 * PTSContinuityHandler - Manages PTS continuity across WebTransport stream boundaries
 *
 * This lightweight handler tracks PTS values across stream transitions and
 * applies adjustments to maintain monotonically increasing timestamps.
 */
class PTSContinuityHandler {
    constructor() {
        this.TAG = 'PTSContinuityHandler';

        // Track last PTS for each stream type
        this._lastPTS = {
            video: null,  // Last video PTS
            audio: null   // Last audio PTS
        };

        // PTS adjustment offsets for each stream type
        this._ptsOffsets = {
            video: 0,
            audio: 0
        };

        // Stream transition detection
        this._streamTransitionDetected = false;
        this._newStreamStarted = false;

        // Typical frame durations (90kHz clock)
        this.TYPICAL_VIDEO_FRAME_DURATION = 3000;  // ~33ms at 30fps
        this.TYPICAL_AUDIO_FRAME_DURATION = 1536;  // ~AAC frame duration

        // PTS wraparound handling (33-bit PTS)
        this.PTS_MAX_VALUE = 8589934591;  // 2^33 - 1
        this.PTS_WRAPAROUND_THRESHOLD = 8589934591 - 90000 * 10;  // 10 seconds before wraparound

        // Continuity reset threshold (ms)
        this.CONTINUITY_RESET_THRESHOLD = 5000;  // 5 seconds
        this._lastStreamTransitionTime = Date.now();

        // Enable detailed logging
        this.enableDetailedLogging = false;
    }

    /**
     * Notifies the handler of a stream transition
     */
    notifyStreamTransition() {
        if (this.enableDetailedLogging) {
            Log.v(this.TAG, `Stream transition detected`);
        }

        this._streamTransitionDetected = true;
        this._newStreamStarted = true;
        this._lastStreamTransitionTime = Date.now();
    }

    /**
     * Process a PTS value for a specific stream type, applying continuity adjustments
     *
     * @param {number} pts The original PTS value
     * @param {string} streamType Either 'video' or 'audio'
     * @return {number} The adjusted PTS value ensuring continuity
     */
    processPTS(pts, streamType) {
        if (pts === null || pts === undefined) {
            return null;
        }

        // Handle potential stream boundary conditions
        if (this._newStreamStarted && this._lastPTS[streamType] !== null) {
            this._calculatePTSOffset(pts, streamType);
            this._newStreamStarted = false;
        }

        // Apply the offset
        const adjustedPTS = this._applyOffset(pts, streamType);

        // Track the last adjusted PTS
        this._lastPTS[streamType] = adjustedPTS;

        return adjustedPTS;
    }

    /**
     * Calculate the PTS offset to maintain continuity when transitioning to a new stream
     * 
     * @param {number} firstPTS The first PTS value in the new stream
     * @param {string} streamType The stream type ('video' or 'audio')
     */
    _calculatePTSOffset(firstPTS, streamType) {
        // We only calculate offset based on the reference stream (video)
        // and then apply the same offset to all streams
        if (streamType !== 'video') {
            return;
        }
        
        const lastPTS = this._lastPTS[streamType];
        
        if (lastPTS === null) {
            return;
        }
        
        // Expected next PTS based on typical frame duration
        const frameDuration = this.TYPICAL_VIDEO_FRAME_DURATION;
        
        let expectedPTS = lastPTS + frameDuration;
        
        // Handle PTS wraparound
        if (expectedPTS > this.PTS_MAX_VALUE) {
            expectedPTS -= this.PTS_MAX_VALUE + 1;
        }
        
        // Calculate offset needed (accounting for wraparound conditions)
        let offset = 0;
        
        if (firstPTS < expectedPTS && (expectedPTS - firstPTS) > this.PTS_MAX_VALUE / 2) {
            // PTS wraparound case: first PTS from new stream is after wraparound
            offset = (expectedPTS - this.PTS_MAX_VALUE - 1) - firstPTS;
        } else if (firstPTS > expectedPTS && (firstPTS - expectedPTS) > this.PTS_MAX_VALUE / 2) {
            // PTS wraparound case: last PTS from old stream is near wraparound
            offset = (expectedPTS + this.PTS_MAX_VALUE + 1) - firstPTS;
        } else {
            // Normal case: no wraparound
            offset = expectedPTS - firstPTS;
        }
        
        // Update the unified offset for all stream types
        // This is the key change - we apply the same offset to all streams
        const unifiedOffset = offset;
        for (const type in this._ptsOffsets) {
            this._ptsOffsets[type] += unifiedOffset;
        }
        
        if (this.enableDetailedLogging) {
            Log.v(this.TAG, `Calculated unified PTS offset: ${unifiedOffset}`);
            Log.v(this.TAG, `Applied to all streams - video: ${this._ptsOffsets.video}, audio: ${this._ptsOffsets.audio}`);
        }
    }

    /**
     * Apply the calculated offset to a PTS value
     *
     * @param {number} pts Original PTS value
     * @param {string} streamType The stream type ('video' or 'audio')
     * @return {number} Adjusted PTS value
     */
    _applyOffset(pts, streamType) {
        let adjustedPTS = pts + this._ptsOffsets[streamType];

        // Handle wraparound for adjusted PTS
        if (adjustedPTS > this.PTS_MAX_VALUE) {
            adjustedPTS -= this.PTS_MAX_VALUE + 1;
        } else if (adjustedPTS < 0) {
            adjustedPTS += this.PTS_MAX_VALUE + 1;
        }

        return adjustedPTS;
    }

    /**
     * Check if PTS value is approaching wraparound
     *
     * @param {number} pts The PTS value to check
     * @return {boolean} True if PTS is approaching wraparound
     */
    isApproachingWraparound(pts) {
        return pts > this.PTS_WRAPAROUND_THRESHOLD;
    }

    /**
     * Reset continuity tracking
     * This should be called when a full reset is needed (e.g., seek operation)
     */
    reset() {
        this._lastPTS = {
            video: null,
            audio: null
        };

        this._ptsOffsets = {
            video: 0,
            audio: 0
        };

        this._streamTransitionDetected = false;
        this._newStreamStarted = false;
        this._lastStreamTransitionTime = Date.now();

        Log.v(this.TAG, `PTS continuity tracking reset`);
    }

    /**
     * Check for conditions that would require a reset of continuity tracking
     * Should be called periodically
     */
    checkForResetConditions() {
        const now = Date.now();
        const timeSinceLastTransition = now - this._lastStreamTransitionTime;

        // Reset if it's been a long time since the last transition
        if (timeSinceLastTransition > this.CONTINUITY_RESET_THRESHOLD) {
            if (this._streamTransitionDetected) {
                this._streamTransitionDetected = false;

                if (this.enableDetailedLogging) {
                    Log.v(this.TAG, `Stream transition resolved, continuity maintained`);
                }
            }
        }
    }

    /**
     * Extract PTS from a TS packet
     *
     * @param {Uint8Array} packet The MPEG-TS packet
     * @return {Object|null} Object containing PTS and stream type, or null if not found
     */
    extractPTSInfo(packet) {
        try {
            // Check if this is the start of PES
            const payloadStart = (packet[1] & 0x40) !== 0;
            const hasAdaptationField = (packet[3] & 0x20) !== 0;
            const hasPayload = (packet[3] & 0x10) !== 0;

            if (!payloadStart || !hasPayload) return null;

            // Calculate payload offset
            const adaptationFieldLength = hasAdaptationField ? packet[4] : 0;
            const payloadOffset = hasAdaptationField ? 5 + adaptationFieldLength : 4;

            // Ensure we have enough bytes for a PES header
            if (packet.length < payloadOffset + 14) return null;

            // Check for PES start code
            if (packet[payloadOffset] !== 0x00 ||
                packet[payloadOffset + 1] !== 0x00 ||
                packet[payloadOffset + 2] !== 0x01) {
                return null;
            }

            // Check stream ID to determine type
            const streamId = packet[payloadOffset + 3];
            let streamType = null;

            if (streamId >= 0xE0 && streamId <= 0xEF) {
                streamType = 'video';
            } else if (streamId >= 0xC0 && streamId <= 0xDF) {
                streamType = 'audio';
            } else {
                return null; // Not a video or audio stream
            }

            // Check PTS_DTS_flags
            const ptsDtsFlags = (packet[payloadOffset + 7] & 0xC0) >> 6;
            if (ptsDtsFlags === 0) return null; // No PTS present

            // Extract PTS
            const ptsOffset = payloadOffset + 9;
            const p0 = packet[ptsOffset];
            const p1 = packet[ptsOffset + 1];
            const p2 = packet[ptsOffset + 2];
            const p3 = packet[ptsOffset + 3];
            const p4 = packet[ptsOffset + 4];

            const pts = (((p0 & 0x0E) << 29) |
                         ((p1 & 0xFF) << 22) |
                         ((p2 & 0xFE) << 14) |
                         ((p3 & 0xFF) << 7) |
                         ((p4 & 0xFE) >> 1));

            return {
                pts: pts,
                streamType: streamType
            };

        } catch (e) {
            return null;
        }
    }
}

/**
 * WebTransportLoader - Loads MPEG-TS data over WebTransport connection
 * Enhanced with PTS continuity maintenance across stream boundaries
 * and improved buffer management for smooth stream transitions
 */
class WebTransportLoader extends BaseLoader {
    constructor(options = {}) {
        super('webtransport-loader');
        this.TAG = 'WebTransportLoader';

        this._needStash = true;
        this._transport = null;
        this._streamReader = null;  // For reading incoming streams
        this._currentStreamReader = null;  // For reading current active stream
        this._requestAbort = false;
        this._receivedLength = 0;

	this._dispatchErrorCount = 0;
	this._maxConsecutiveErrors = 5;
	this._dispatchErrorBackoffTime = 0;

	this._savedInitSegments = { video: null, audio: null };
	this._hasReceivedInitSegments = false;

        // Configure settings with option overrides
        this.config = {
            // Logging
            enableDetailedLogging: options.enableDetailedLogging !== undefined ? 
                                  options.enableDetailedLogging : true,
            
	     // Buffer settings
	    bufferSizeInPackets: 800,              // Increase from 400 to 800
	    minBufferSizeInPackets: 400,           // Increase from 200 to 400
	    maxBufferSize: 1200,                   // Increase from 1000 to 1200
	    forceDispatchThreshold: 1000,          // Increase from 600 to 1000

            // Playback settings
            enablePTSContinuity: options.enablePTSContinuity !== undefined ? 
                                options.enablePTSContinuity : true,
            initialBufferingThreshold: options.initialBufferingThreshold || 0.95,
            
            // Dispatch settings
            dispatchChunkSize: options.dispatchChunkSize || 75,
            keyframeAwareChunking: options.keyframeAwareChunking !== undefined ? 
                                 options.keyframeAwareChunking : true,
            keyframeDetectionRetries: options.keyframeDetectionRetries || 3,
            
            // Network adaptation
            enableNetworkAdaptation: options.enableNetworkAdaptation !== undefined ? 
                                   options.enableNetworkAdaptation : true,
            adaptationInterval: options.adaptationInterval || 10000,    // Adapt every 10 seconds
            
            // Performance settings
            enablePerformanceMonitoring: options.enablePerformanceMonitoring !== undefined ?
                                       options.enablePerformanceMonitoring : true,
            performanceMonitoringInterval: options.performanceMonitoringInterval || 5000 // Check every 5 seconds
        };

        // Packet buffering
        this.PACKET_SIZE = 188;
        this._packetBuffer = [];
        this._initialBufferingComplete = false;

        // Diagnostics
        this.stats = {
            totalBytesReceived: 0,
            totalPacketsProcessed: 0,
            streamsReceived: 0,
            lastPTS: {
                video: null,
                audio: null
            },
            ptsAdjustments: {
                video: 0,
                audio: 0
            },
            bufferFullness: 0,
            maxBufferSize: this.config.maxBufferSize,
            keyframesDetected: 0,
            chunksDispatched: 0,
            
            // Performance metrics
            dispatchHistory: [],        // Array of dispatch timestamps and sizes
            networkCondition: 'good',   // 'good', 'variable', or 'poor'
            avgDispatchRate: 0,         // Average packets per second
            avgDispatchSize: 0,         // Average dispatch size
            lastDispatchTime: 0,        // Last dispatch timestamp
            
            // Network metrics
            bytesPerSecond: 0,
            packetJitter: 0,            // Variability in packet arrival time
            streamTransitionCount: 0    // Number of stream transitions
        };

        // Keyframe tracking
        this._keyframePositions = [];
        this._lastDispatchedKeyframeIndex = -1;

        // Initialize PTS continuity handler
        this._ptsHandler = new PTSContinuityHandler();
        this._ptsHandler.enableDetailedLogging = this.config.enableDetailedLogging;
        
        // Performance monitoring
        this._lastPerformanceCheck = Date.now();
        this._setupPerformanceMonitoring();
    }

    /**
     * Set up performance monitoring for adaptive buffer management
     */
    _setupPerformanceMonitoring() {
        if (!this.config.enablePerformanceMonitoring) return;

        this._performanceMonitorTimer = setInterval(() => {
            this._evaluatePerformance();
        }, this.config.performanceMonitoringInterval);
    }

    /**
     * Evaluate playback performance and adjust buffer sizes if needed
     */
    _evaluatePerformance() {
        if (!this.config.enableNetworkAdaptation) return;

        const now = Date.now();
        const timeSinceLastCheck = now - this._lastPerformanceCheck;
        this._lastPerformanceCheck = now;

        // Calculate bytes per second
        if (timeSinceLastCheck > 0) {
            this.stats.bytesPerSecond = (this._receivedLength / timeSinceLastCheck) * 1000;
        }

        // Analyze dispatch history
        if (this.stats.dispatchHistory.length > 5) {
            // Calculate average dispatch interval
            let totalInterval = 0;
            let intervalCount = 0;
            let intervals = [];

            for (let i = 1; i < this.stats.dispatchHistory.length; i++) {
                const interval = this.stats.dispatchHistory[i].time - this.stats.dispatchHistory[i-1].time;
                if (interval > 0) {
                    totalInterval += interval;
                    intervals.push(interval);
                    intervalCount++;
                }
            }

            // Calculate jitter (standard deviation of intervals)
            if (intervalCount > 3) {
                const avgInterval = totalInterval / intervalCount;
                let sumSquaredDiff = 0;

                for (let interval of intervals) {
                    sumSquaredDiff += Math.pow(interval - avgInterval, 2);
                }

                this.stats.packetJitter = Math.sqrt(sumSquaredDiff / intervalCount);

                // Determine network condition
                if (this.stats.packetJitter > 500) { // High jitter
                    this.stats.networkCondition = 'poor';
                } else if (this.stats.packetJitter > 200) {
                    this.stats.networkCondition = 'variable';
                } else {
                    this.stats.networkCondition = 'good';
                }

                // Adjust buffer size based on network condition
                this._adjustBufferSize();
            }

            // Keep history limited
            if (this.stats.dispatchHistory.length > 20) {
                this.stats.dispatchHistory = this.stats.dispatchHistory.slice(-20);
            }
        }
    }

    /**
     * Adjust buffer size based on network conditions and playback performance
     */
    _adjustBufferSize() {
        const currentSize = this.config.bufferSizeInPackets;
        let newSize = currentSize;

        switch (this.stats.networkCondition) {
            case 'poor':
                // Increase buffer for poor conditions
                newSize = Math.min(this.config.maxBufferSize, currentSize * 1.5);
                break;

            case 'variable':
                // Moderately increase buffer
                newSize = Math.min(this.config.maxBufferSize, currentSize * 1.2);
                break;

            case 'good':
                // Slightly reduce buffer if it's very large
                if (currentSize > this.config.minBufferSizeInPackets * 2) {
                    newSize = Math.max(this.config.minBufferSizeInPackets, currentSize * 0.9);
                }
                break;
        }

        // Only update if the change is significant (greater than 10%)
        if (Math.abs(newSize - currentSize) / currentSize > 0.1) {
            this.adjustBufferSize(Math.round(newSize));
        }
    }

    /**
     * Adjusts the buffer size based on network conditions or playback requirements
     *
     * @param {number} newSizeInPackets New buffer size in packets
     */
    adjustBufferSize(newSizeInPackets) {
        // Ensure buffer size is within bounds
        newSizeInPackets = Math.max(this.config.minBufferSizeInPackets,
                            Math.min(this.config.maxBufferSize, newSizeInPackets));

        if (newSizeInPackets === this.config.bufferSizeInPackets) {
            return; // No change needed
        }

        const oldSize = this.config.bufferSizeInPackets;
        this.config.bufferSizeInPackets = newSizeInPackets;

        // Adjust related thresholds
        this.config.forceDispatchThreshold = Math.min(
            this.config.maxBufferSize - 200,
            Math.max(this.config.bufferSizeInPackets + 200, this.config.forceDispatchThreshold)
        );

        this.stats.bufferFullness = this._packetBuffer.length / newSizeInPackets;

        Log.v(this.TAG, `Buffer size adjusted from ${oldSize} to ${newSizeInPackets} packets`);
        Log.v(this.TAG, `Force dispatch threshold adjusted to ${this.config.forceDispatchThreshold} packets`);

        // If buffer is now too large, dispatch excess packets
        if (this._packetBuffer.length > newSizeInPackets && this._initialBufferingComplete) {
            const excess = this._packetBuffer.length - newSizeInPackets;
            this._dispatchPacketChunk(excess);
        }
    }

    /**
     * Record a dispatch event for performance monitoring
     *
     * @param {number} packetCount Number of packets dispatched
     */
    _recordDispatch(packetCount) {
        if (!this.config.enablePerformanceMonitoring) return;

        const now = Date.now();
        this.stats.dispatchHistory.push({
            time: now,
            packets: packetCount
        });

        this.stats.lastDispatchTime = now;
        this.stats.chunksDispatched++;

        // Update average dispatch size
        const totalDispatches = this.stats.dispatchHistory.length;
        if (totalDispatches > 0) {
            let totalPackets = 0;
            for (let dispatch of this.stats.dispatchHistory) {
                totalPackets += dispatch.packets;
            }
            this.stats.avgDispatchSize = totalPackets / totalDispatches;
        }
    }

	/**
	 * Dispatch a specific number of packets from the buffer
	 * Enhanced with basic error detection and backoff
	 *
	 * @param {number} packetCount Number of packets to dispatch
	 */
	_dispatchPacketChunk(packetCount) {
	    if (this._packetBuffer.length === 0 || packetCount <= 0) return;

	    // Limit to available packets
	    const actualPacketsToDispatch = Math.min(packetCount, this._packetBuffer.length);

	    // Get packets to dispatch
	    const packetsToDispatch = this._packetBuffer.slice(0, actualPacketsToDispatch);

	    // Remove those packets from the buffer
	    this._packetBuffer = this._packetBuffer.slice(actualPacketsToDispatch);

	    // Combine packets into a single chunk
	    const totalLength = packetsToDispatch.reduce((sum, packet) => sum + packet.length, 0);
	    const chunk = new Uint8Array(totalLength);

	    let offset = 0;
	    packetsToDispatch.forEach(packet => {
		chunk.set(packet, offset);
		offset += packet.length;
	    });

	    // Log dispatch details if detailed logging is enabled
	    if (this.config.enableDetailedLogging) {
		Log.v(this.TAG, `Dispatching ${actualPacketsToDispatch} packets (${(totalLength / 1024).toFixed(1)} KB), remaining buffer: ${this._packetBuffer.length} packets`);
	    }

	    // Send to demuxer with error detection
	    if (this._onDataArrival) {
		try {
		    this._onDataArrival(chunk.buffer, 0, totalLength);
		    
		    // Reset error count on success
		    if (this._dispatchErrorCount > 0) {
			Log.v(this.TAG, `Dispatch succeeded after ${this._dispatchErrorCount} errors`);
			this._dispatchErrorCount = 0;
			this._dispatchErrorBackoffTime = 0;
		    }
		} catch (e) {
		    // Increment error count
		    this._dispatchErrorCount = (this._dispatchErrorCount || 0) + 1;
		    const maxErrors = this._maxConsecutiveErrors || 5;
		    
		    Log.w(this.TAG, `Dispatch error ${this._dispatchErrorCount}/${maxErrors}: ${e.message}`);
		    
		    // If we hit too many errors, pause dispatching temporarily
		    if (this._dispatchErrorCount >= maxErrors) {
			// Calculate backoff time with exponential increase
			this._dispatchErrorBackoffTime = this._dispatchErrorBackoffTime ? 
			    Math.min(10000, this._dispatchErrorBackoffTime * 2) : 1000;
			
			Log.w(this.TAG, `Too many consecutive errors, pausing dispatch for ${this._dispatchErrorBackoffTime}ms`);
			
			// Put packets back in buffer
			this._packetBuffer = [...packetsToDispatch, ...this._packetBuffer];
			
			// Update stats
			this.stats.bufferFullness = this._packetBuffer.length / this.config.bufferSizeInPackets;
			
			// Use setTimeout for backoff, which avoids creating a class member timer
			setTimeout(() => {
			    Log.v(this.TAG, `Resuming dispatch after ${this._dispatchErrorBackoffTime}ms error backoff`);
			    
			    // Try again with a smaller chunk
			    if (this._packetBuffer.length > 0) {
				const testDispatchSize = Math.min(20, this._packetBuffer.length);
				this._dispatchPacketChunk(testDispatchSize);
			    }
			}, this._dispatchErrorBackoffTime);
			
			return; // Skip further processing
		    }
		    
		    // If we're still within tolerable error count, continue normally
		    // with the understanding that this dispatch failed but we'll try again
		}
	    }

	    // Update stats after dispatch - only if we didn't return early due to error
	    this.stats.bufferFullness = this._packetBuffer.length / this.config.bufferSizeInPackets;
	    this._recordDispatch(actualPacketsToDispatch);
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

            // Create WebTransport connection
            this._transport = new WebTransport(dataSource.url);
            await this._transport.ready;

            // Set up error and close handlers
            this._transport.closed.then(info => {
                Log.v(this.TAG, `WebTransport connection closed: ${JSON.stringify(info)}`);
                if (this._status !== LoaderStatus.kComplete && this._status !== LoaderStatus.kError) {
                    this._status = LoaderStatus.kError;
                    if (this._onError) {
                        this._onError(LoaderErrors.CONNECTION_ERROR, {
                            code: -1,
                            msg: `WebTransport connection closed: ${JSON.stringify(info)}`
                        });
                    }
                }
            }).catch(error => {
                Log.e(this.TAG, `WebTransport connection error: ${error.message}`);
            });

            // Start processing incoming streams
            this._status = LoaderStatus.kBuffering;
            this._streamReader = this._transport.incomingUnidirectionalStreams.getReader();

            // Reset buffer state
            this._packetBuffer = [];
            this._initialBufferingComplete = false;
            this.stats.maxBufferSize = this.config.bufferSizeInPackets;

            // Start processing streams
            this._processStreams();

            // Set up diagnostics timer if needed
            this.diagnosticTimer = setInterval(() => {
                this._logDiagnostics();

                // Check for PTS continuity reset conditions
                if (this.config.enablePTSContinuity) {
                    this._ptsHandler.checkForResetConditions();
                }
            }, 10000);

        } catch (e) {
            this._status = LoaderStatus.kError;
            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
            }
        }
    }

    async _processStreams() {
        try {
            while (!this._requestAbort) {
                // Get the next stream
                const { value: stream, done } = await this._streamReader.read();

                if (done) {
                    Log.v(this.TAG, "No more incoming streams available");
                    break;
                }

                if (!stream) {
                    Log.w(this.TAG, "Received null stream, waiting for next stream");
                    continue;
                }

                this.stats.streamsReceived++;
                Log.v(this.TAG, `Received stream #${this.stats.streamsReceived}, processing...`);

                // Log buffer status at stream transition for better debugging
                Log.v(this.TAG, `Buffer status at stream transition: ${this._packetBuffer.length} packets (${(this._packetBuffer.length * this.PACKET_SIZE / 1024).toFixed(1)} KB)`);

                // Notify PTS handler about stream transition if enabled
                if (this.config.enablePTSContinuity) {
                    this._ptsHandler.notifyStreamTransition();
                }

                // Process this stream until it's done
                this._currentStreamReader = stream.getReader();
                await this._readStreamData(this._currentStreamReader);
            }

            // Final flush of any remaining packets before closing
            if (this._packetBuffer.length > 0 && !this._requestAbort) {
                Log.v(this.TAG, `Final flush of remaining ${this._packetBuffer.length} packets`);
                this._dispatchPacketChunk(this._packetBuffer.length);
            }

            Log.v(this.TAG, "Stream processing loop ended");

        } catch (e) {
            if (!this._requestAbort) {
                Log.e(this.TAG, `Error in _processStreams: ${e.message}`);
                this._status = LoaderStatus.kError;
                if (this._onError) {
                    this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
                }
            }
        }
    }

    _findSyncByteAlignment(buffer) {
        // Try to find a pattern of sync bytes at PACKET_SIZE intervals
        for (let i = 0; i <= buffer.length - this.PACKET_SIZE * 2; i++) {
            if (buffer[i] === 0x47) {  // MPEG-TS sync byte
                // Check for multiple sync bytes at expected intervals
                let validSyncCount = 1;
                for (let j = 1; j <= 3; j++) {
                    const nextSyncPos = i + (j * this.PACKET_SIZE);
                    if (nextSyncPos < buffer.length && buffer[nextSyncPos] === 0x47) {
                        validSyncCount++;
                    } else {
                        break;
                    }
                }

                if (validSyncCount >= 2) {
                    return i;
                }
            }
        }

        // Fallback: return position of first sync byte
        for (let i = 0; i < buffer.length; i++) {
            if (buffer[i] === 0x47) {
                return i;
            }
        }

        return -1;  // No sync byte found
    }

async _readStreamData(reader) {
        let pendingData = new Uint8Array(0);

        try {
            while (!this._requestAbort) {
                const { value, done } = await reader.read();

                if (done) {
                    Log.v(this.TAG, "Stream ended, processing any remaining data");

                    // Process any remaining complete packets without forcing a flush
                    if (pendingData.length >= this.PACKET_SIZE) {
                        const completePacketsLength = Math.floor(pendingData.length / this.PACKET_SIZE) * this.PACKET_SIZE;
                        this._processChunk(pendingData.slice(0, completePacketsLength));
                    }

                    // Note: We're intentionally NOT flushing remaining packets here
                    // to maintain buffer continuity across stream transitions

                    // After a stream ends, force a sanity check on keyframe detection
                    // This helps if we've accumulated a lot of data but haven't detected keyframes properly
                    if (this.config.keyframeAwareChunking &&
                        this._packetBuffer.length > this.config.bufferSizeInPackets * 1.5 &&
                        this._keyframePositions.length < 2) {

                        Log.w(this.TAG, `Stream ended with large buffer (${this._packetBuffer.length} packets) ` +
                             `but few keyframes (${this._keyframePositions.length}). Forcing keyframe scan.`);

                        // Rescan buffer for keyframes
                        this._rescanBufferForKeyframes();

                        // If we still don't have enough keyframes, force a non-keyframe dispatch
                        if (this._keyframePositions.length < 2 && this._packetBuffer.length > this.config.bufferSizeInPackets) {
                            const excessPackets = this._packetBuffer.length - this.config.bufferSizeInPackets;
                            Log.w(this.TAG, `Still insufficient keyframes after scan. ` +
                                 `Forcing dispatch of ${excessPackets} packets.`);
                            this._dispatchPacketChunk(excessPackets);
                        }
                    }
                    
                    return; // Stream is done
                }
                
                if (value) {
                    const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);

                    // Update stats
                    this.stats.totalBytesReceived += chunk.length;
                    this._receivedLength += chunk.length;

                    // Combine with any pending data
                    let combinedChunk;
                    if (pendingData.length > 0) {
                        combinedChunk = new Uint8Array(pendingData.length + chunk.length);
                        combinedChunk.set(pendingData, 0);
                        combinedChunk.set(chunk, pendingData.length);
                    } else {
                        combinedChunk = chunk;
                    }

                    // Find sync byte alignment
                    const syncIndex = this._findSyncByteAlignment(combinedChunk);

                    if (syncIndex === -1) {
                        // No valid sync pattern found, store for next iteration
                        pendingData = combinedChunk;
                        continue;
                    } else if (syncIndex > 0) {
                        // Skip data before the first valid sync byte
                        combinedChunk = combinedChunk.slice(syncIndex);
                    }

                    // Extract complete packets
                    const completePacketsLength = Math.floor(combinedChunk.length / this.PACKET_SIZE) * this.PACKET_SIZE;

                    if (completePacketsLength > 0) {
                        // Process the aligned, complete packets
                        this._processChunk(combinedChunk.slice(0, completePacketsLength));
                    }

                    // Store any remaining bytes for next iteration
                    pendingData = combinedChunk.slice(completePacketsLength);
                }
            }
        } catch (e) {
            if (!this._requestAbort) {
                Log.e(this.TAG, `Error in _readStreamData: ${e.message}`);
            }
        }
    }
    
    /**
     * Process a chunk of MPEG-TS data and manage buffer
     * 
     * @param {Uint8Array} chunk The chunk of MPEG-TS data to process
     */
    _processChunk(chunk) {
        if (!chunk || chunk.length === 0) return;

        // Extract and validate TS packets
        for (let i = 0; i < chunk.length; i += this.PACKET_SIZE) {
            if (i + this.PACKET_SIZE <= chunk.length) {
                const packet = chunk.slice(i, i + this.PACKET_SIZE);

                // Basic validation - check sync byte
                if (packet[0] === 0x47) {
                    // Process PTS information and detect keyframes
                    if (this.config.enablePTSContinuity) {
                        // Pass current buffer position for keyframe tracking
                        this._processPTSInPacket(packet, this._packetBuffer.length);
                    }

                    this._packetBuffer.push(packet);
                    this.stats.totalPacketsProcessed++;

                    // Enforce absolute maximum buffer size
                    if (this._packetBuffer.length >= this.config.maxBufferSize) {
                        Log.w(this.TAG, `Buffer reached maximum size (${this._packetBuffer.length} packets), forcing dispatch`);

                        // Force dispatch - prefer keyframe-aware if possible
                        if (this.config.keyframeAwareChunking && this._keyframePositions.length > 1) {
                            this._dispatchKeyframeAwareChunk();
                        } else {
                            // Fallback to regular dispatch
                            const packetsToDispatch = this._packetBuffer.length - this.config.bufferSizeInPackets;
                            this._dispatchPacketChunk(Math.max(packetsToDispatch, 50)); // At least 50 packets
                        }
                    }
                }
            }
        }

        // Update buffer fullness stats
        this.stats.bufferFullness = this._packetBuffer.length / this.config.bufferSizeInPackets;

        // Check if initial buffering is complete
        if (!this._initialBufferingComplete) {
            // Only start playback when buffer is filled to the specified threshold
            // AND we have at least one keyframe in the buffer
            if (this.stats.bufferFullness >= this.config.initialBufferingThreshold &&
                (!this.config.keyframeAwareChunking || this._keyframePositions.length > 0)) {

                this._initialBufferingComplete = true;
                this._status = LoaderStatus.kComplete;
                Log.v(this.TAG, `Initial buffering complete: ${Math.round(this.stats.bufferFullness * 100)}% full with ${this._keyframePositions.length} keyframes`);
            } else {
                // Still in initial buffering phase
                return; // Don't dispatch packets yet
            }
        }

        // If keyframe detection is enabled but we don't have any keyframes yet,
        // perform a scan when buffer reaches a certain size
        if (this.config.keyframeAwareChunking && 
            this._keyframePositions.length === 0 && 
            this._packetBuffer.length > 100) {
            this._rescanBufferForKeyframes();
        }

        // Enhanced dispatch logic with forced dispatch for overly full buffers
        if (this._packetBuffer.length > this.config.forceDispatchThreshold) {
            // Buffer is extremely full, force dispatch
            Log.w(this.TAG, `Buffer exceeding force threshold (${this._packetBuffer.length}/${this.config.forceDispatchThreshold}), forcing dispatch`);

            if (this.config.keyframeAwareChunking && this._keyframePositions.length > 1) {
                this._dispatchKeyframeAwareChunk();
            } else {
                // Fallback to regular dispatch
                const packetsToDispatch = this._packetBuffer.length - this.config.bufferSizeInPackets;
                this._dispatchPacketChunk(Math.max(packetsToDispatch, 50)); // At least 50 packets
            }
        }
        // Normal dispatch logic - only if buffer exceeds target size
        else if (this._packetBuffer.length > this.config.bufferSizeInPackets) {
            if (this.config.keyframeAwareChunking) {
                this._dispatchKeyframeAwareChunk();
            } else {
                // Original excess-based dispatch
                const excessPackets = this._packetBuffer.length - this.config.bufferSizeInPackets;
                const packetsToDispatch = Math.min(excessPackets, this.config.dispatchChunkSize);

                if (this.config.enableDetailedLogging) {
                    Log.v(this.TAG, `Buffer exceeds target (${this._packetBuffer.length}/${this.config.bufferSizeInPackets}), dispatching ${packetsToDispatch} packets`);
                }

                this._dispatchPacketChunk(packetsToDispatch);
            }
        }
    }
    
    /**
     * Process PTS information in a packet and detect keyframes
     * Enhanced with more aggressive keyframe detection methods
     *
     * @param {Uint8Array} packet MPEG-TS packet
     * @param {number} packetIndex Current index in the buffer
     * @returns {boolean} True if this packet contains a keyframe
     */
    _processPTSInPacket(packet, packetIndex) {
        const ptsInfo = this._ptsHandler.extractPTSInfo(packet);
        let isKeyframe = false;

        // Check for keyframe using multiple methods
        isKeyframe = this._isKeyframeByRAI(packet) || 
                    this._isKeyframeByNAL(packet) || 
                    this._isKeyframeByPESHeader(packet);

        if (isKeyframe && this.config.keyframeAwareChunking) {
            this._keyframePositions.push(packetIndex);
            this.stats.keyframesDetected++;

            if (this.config.enableDetailedLogging && 
                (this._keyframePositions.length <= 5 || this._keyframePositions.length % 5 === 0)) {
                Log.v(this.TAG, `Keyframe detected at packet index ${packetIndex}, total keyframes: ${this.stats.keyframesDetected}`);
            }
        }

        if (ptsInfo) {
            // Process the PTS value through the continuity handler
            const adjustedPTS = this._ptsHandler.processPTS(ptsInfo.pts, ptsInfo.streamType);

            // Update stats
            if (adjustedPTS !== null) {
                this.stats.lastPTS[ptsInfo.streamType] = adjustedPTS;
                this.stats.ptsAdjustments[ptsInfo.streamType] = this._ptsHandler._ptsOffsets[ptsInfo.streamType];
            }
        }

        return isKeyframe;
    }
    

_dispatchKeyframeAwareChunk() {
    // Initialize state tracking variables if they don't exist yet
    if (this._bufferState === undefined) {
        // Initialize dual buffer system
        this._bufferState = {
            // Currently active buffer (being dispatched)
            activeBuffer: this._packetBuffer,
            // Buffer for the next stream (pending)
            pendingBuffer: [],
            // Track keyframes for each buffer
            activeKeyframePositions: this._keyframePositions || [],
            pendingKeyframePositions: [],
            // Stream counters and state tracking
            streamCounter: 1,
            transitionPending: false,
            drainActive: false,
            // Store initialization segments for potential reuse
            savedInitSegments: null,
            // Delay between dispatches to throttle during high load
            lastDispatchTime: 0,
            minDispatchInterval: 0
        };
        
        // Point the main buffer references to the active buffer
        this._packetBuffer = this._bufferState.activeBuffer;
        this._keyframePositions = this._bufferState.activeKeyframePositions;
    }
    
    // HANDLE STREAM TRANSITION
    if (this._ptsHandler._newStreamStarted) {
        this._bufferState.streamCounter++;
        Log.v(this.TAG, `Stream transition to stream #${this._bufferState.streamCounter} detected`);
        
        // If this is our first stream (and init segments not saved yet), save a buffer snapshot
        if (this._bufferState.streamCounter === 2 && !this._bufferState.savedInitSegments && this._bufferState.activeBuffer.length > 300) {
            // Create a deep copy of initialization segments
            const packetsToSave = Math.min(800, this._bufferState.activeBuffer.length);
            this._bufferState.savedInitSegments = [];
            
            for (let i = 0; i < packetsToSave; i++) {
                // Create a new copy of each packet
                const packetCopy = new Uint8Array(this._bufferState.activeBuffer[i]);
                this._bufferState.savedInitSegments.push(packetCopy);
            }
            
            Log.v(this.TAG, `Saved ${packetsToSave} packets from first stream for future initialization`);
        }
        
        // Activate the dual buffer mode
        this._bufferState.transitionPending = true;
        this._bufferState.drainActive = true;
        
        // Start using the pending buffer for the new stream
        this._packetBuffer = this._bufferState.pendingBuffer;
        this._keyframePositions = this._bufferState.pendingKeyframePositions;
        
        // Clear the PTS handler flag immediately
        this._ptsHandler._newStreamStarted = false;
        
        Log.v(this.TAG, `Switched to pending buffer for stream #${this._bufferState.streamCounter}. Will drain active buffer first.`);
        
        return;
    }
    
    // SPECIAL HANDLING FOR DRAIN MODE
    if (this._bufferState.drainActive) {
        // Check if active buffer is empty or near-empty (less than 50 packets)
        if (this._bufferState.activeBuffer.length <= 50) {
            // Almost drained, prepare to switch to the pending buffer
            
            // 1. If we have saved init segments, queue them up to be sent first
            if (this._bufferState.savedInitSegments && this._bufferState.savedInitSegments.length > 0) {
                Log.v(this.TAG, `Stream #${this._bufferState.streamCounter}: Preparing initialization (${this._bufferState.savedInitSegments.length} packets)`);
                
                // Send up to 200 packets of initialization segments at once
                const initChunkSize = Math.min(200, this._bufferState.savedInitSegments.length);
                const initSegmentsToSend = this._bufferState.savedInitSegments.slice(0, initChunkSize);
                this._bufferState.savedInitSegments = this._bufferState.savedInitSegments.slice(initChunkSize);
                
                // Create a buffer from the initialization segments
                const totalSize = initSegmentsToSend.reduce((sum, p) => sum + p.byteLength, 0);
                const combinedBuffer = new Uint8Array(totalSize);
                
                let offset = 0;
                for (const packet of initSegmentsToSend) {
                    combinedBuffer.set(packet, offset);
                    offset += packet.byteLength;
                }
                
                // Send the initialization segments
                if (this._onDataArrival) {
                    try {
                        this._onDataArrival(combinedBuffer.buffer, 0, combinedBuffer.byteLength);
                        Log.v(this.TAG, `Successfully sent ${totalSize} bytes of saved initialization data`);
                    } catch (e) {
                        Log.e(this.TAG, `Error sending saved init data: ${e.message}`);
                    }
                }
                
                // If we still have more init segments to send, don't complete the transition yet
                if (this._bufferState.savedInitSegments.length > 0) {
                    return;
                }
            }
            
            // 2. Now complete the switch to the pending buffer
            Log.v(this.TAG, `Stream #${this._bufferState.streamCounter}: Active buffer drained, switching to new stream data`);
            
            // Make the pending buffer the new active buffer
            this._bufferState.activeBuffer = this._bufferState.pendingBuffer;
            this._bufferState.activeKeyframePositions = this._bufferState.pendingKeyframePositions;
            
            // Create new empty pending buffer for future transitions
            this._bufferState.pendingBuffer = [];
            this._bufferState.pendingKeyframePositions = [];
            
            // Update references
            this._packetBuffer = this._bufferState.activeBuffer;
            this._keyframePositions = this._bufferState.activeKeyframePositions;
            
            // Exit drain mode
            this._bufferState.drainActive = false;
            this._bufferState.transitionPending = false;
            
            // Scan for keyframes in the new active buffer
            this._rescanBufferForKeyframes();
            
            Log.v(this.TAG, `Stream #${this._bufferState.streamCounter}: Transition complete, new buffer size: ${this._packetBuffer.length} packets`);
            
            // Wait a short time before dispatching from the new buffer to allow initialization
            this._bufferState.lastDispatchTime = Date.now();
            this._bufferState.minDispatchInterval = 100; // 100ms throttle for new stream
            
            return;
        }
        
        // If we're still draining, focus on emptying the active buffer
        // Dispatch larger chunks to empty it faster
        const drainChunkSize = Math.min(250, this._bufferState.activeBuffer.length);
        
        if (drainChunkSize > 0) {
            Log.v(this.TAG, `Draining active buffer: ${drainChunkSize} packets, ${this._bufferState.activeBuffer.length - drainChunkSize} remaining`);
            
            // Keep a reference to the current buffer setup
            const currentPacketBuffer = this._packetBuffer;
            const currentKeyframePositions = this._keyframePositions;
            
            // Temporarily switch to the active buffer for dispatching
            this._packetBuffer = this._bufferState.activeBuffer;
            this._keyframePositions = this._bufferState.activeKeyframePositions;
            
            // Dispatch from the active buffer
            this._dispatchPacketChunk(drainChunkSize);
            
            // Update the active keyframe positions after dispatch
            this._bufferState.activeKeyframePositions = this._keyframePositions
                .map(pos => pos)
                .filter(pos => pos >= 0 && pos < this._bufferState.activeBuffer.length);
            
            // Switch back to the buffer we were working with
            this._packetBuffer = currentPacketBuffer;
            this._keyframePositions = currentKeyframePositions;
            
            // Add a small delay to prevent overwhelming the renderer
            this._bufferState.lastDispatchTime = Date.now();
            
            return;
        }
        
        return;
    }
    
    // DISPATCH THROTTLING - Avoid too frequent dispatches during high load periods
    const now = Date.now();
    if ((now - this._bufferState.lastDispatchTime) < this._bufferState.minDispatchInterval) {
        // Skip this dispatch cycle to prevent overwhelming MSE
        return;
    }
    
    // Reset dispatch interval for normal operation (only temporarily increase it during transitions)
    this._bufferState.minDispatchInterval = 0;
    
    // STANDARD KEYFRAME-BASED DISPATCH (Normal operation mode)
    const numKeyframes = this._keyframePositions.length;
    
    // If no keyframes but buffer getting large, force a rescan
    if (numKeyframes === 0 && this._packetBuffer.length > this.config.bufferSizeInPackets) {
        Log.v(this.TAG, `No keyframes in buffer of ${this._packetBuffer.length} packets. Forcing keyframe scan.`);
        this._rescanBufferForKeyframes();
        
        // If still no keyframes, dispatch a reasonable amount
        if (this._keyframePositions.length === 0 && 
            this._packetBuffer.length > this.config.bufferSizeInPackets * 1.2) {
            const packetsToDispatch = Math.min(
                Math.floor(this._packetBuffer.length / 3),
                300
            );
            Log.v(this.TAG, `Still no keyframes after scan. Dispatching ${Math.floor(packetsToDispatch)} packets.`);
            this._dispatchPacketChunk(Math.floor(packetsToDispatch));
            this._bufferState.lastDispatchTime = now;
            return;
        }
    }
    
    // Standard case: If we have at least two keyframes, dispatch a complete segment
    if (numKeyframes >= 2) {
        let startKeyframeIndex = 0;
        const startPos = this._keyframePositions[startKeyframeIndex];
        
        // In most cases, we'll just dispatch from first keyframe to second keyframe
        const endKeyframeIndex = 1;
        const endPos = this._keyframePositions[endKeyframeIndex];
        const packetsToDispatch = endPos;
        
        if (this.config.enableDetailedLogging) {
            Log.v(this.TAG, `Keyframe-aware dispatch: from keyframe ${startKeyframeIndex} to ${endKeyframeIndex} ` +
                 `(${packetsToDispatch} packets), maintaining ${this._packetBuffer.length - endPos} in buffer`);
        }
        
        // Dispatch the segment
        this._dispatchPacketChunk(packetsToDispatch);
        this._bufferState.lastDispatchTime = now;
        
        // Update keyframe positions to account for removed packets
        this._keyframePositions = this._keyframePositions
            .slice(endKeyframeIndex)
            .map(pos => pos - endPos)
            .filter(pos => pos >= 0 && pos < this._packetBuffer.length);
            
        return;
    }
    
    // Handle the case where we have only one keyframe:
    if (numKeyframes === 1) {
        const keyframePos = this._keyframePositions[0];
        
        // If the keyframe is not at the beginning, dispatch data up to the keyframe
        if (keyframePos > 0) {
            Log.v(this.TAG, `Dispatching ${keyframePos} packets before keyframe`);
            this._dispatchPacketChunk(keyframePos);
            this._bufferState.lastDispatchTime = now;
            
            // Update keyframe positions
            this._keyframePositions[0] = 0;
            return;
        }
        
        // If buffer is getting too large with just one keyframe, dispatch some data
        if (this._packetBuffer.length > this.config.bufferSizeInPackets * 1.2) {
            const packetsToDispatch = Math.min(
                Math.floor(this._packetBuffer.length / 3),
                250
            );
            Log.v(this.TAG, `Buffer large with one keyframe. Dispatching ${packetsToDispatch} packets.`);
            this._dispatchPacketChunk(packetsToDispatch);
            this._bufferState.lastDispatchTime = now;
            
            // Reset keyframe positions
            this._keyframePositions = [];
            this._rescanBufferForKeyframes();
            return;
        }
    }
    
    // Force dispatch if buffer exceeds threshold
    if (this._packetBuffer.length > this.config.forceDispatchThreshold) {
        const excessPackets = Math.min(
            this._packetBuffer.length - this.config.bufferSizeInPackets,
            300
        );
        Log.v(this.TAG, `Buffer exceeding threshold (${this._packetBuffer.length}/${this.config.forceDispatchThreshold}). ` +
             `Forcing dispatch of ${excessPackets} packets.`);
        this._dispatchPacketChunk(excessPackets);
        this._bufferState.lastDispatchTime = now;
        
        // Reset keyframe detection after forced dispatch
        this._keyframePositions = [];
        this._rescanBufferForKeyframes();
        return;
    }
    
    // If we reach here with no keyframes and buffer is not too full, wait for more data
    if (numKeyframes === 0 && this._packetBuffer.length < this.config.bufferSizeInPackets * 1.5) {
        if (this.config.enableDetailedLogging) {
            Log.v(this.TAG, `No keyframes detected yet, waiting (buffer: ${this._packetBuffer.length} packets)`);
        }
    }
}

/**
 * Process a chunk of MPEG-TS data and manage buffer
 * Modified to handle dual buffer system
 * 
 * @param {Uint8Array} chunk The chunk of MPEG-TS data to process
 */
_processChunk(chunk) {
    if (!chunk || chunk.length === 0) return;

    // Extract and validate TS packets
    for (let i = 0; i < chunk.length; i += this.PACKET_SIZE) {
        if (i + this.PACKET_SIZE <= chunk.length) {
            const packet = chunk.slice(i, i + this.PACKET_SIZE);

            // Basic validation - check sync byte
            if (packet[0] === 0x47) {
                // During stream transition, route new packets to the correct buffer
                let targetBuffer = this._packetBuffer; // Points to current active or pending buffer
                let targetKeyframePositions = this._keyframePositions;
                
                // Process PTS information and detect keyframes
                if (this.config.enablePTSContinuity) {
                    // Pass current buffer position for keyframe tracking
                    const isKeyframe = this._processPTSInPacket(packet, targetBuffer.length);
                    
                    // Add keyframe position if detected
                    if (isKeyframe && this.config.keyframeAwareChunking) {
                        targetKeyframePositions.push(targetBuffer.length);
                    }
                }

                // Add the packet to the target buffer
                targetBuffer.push(packet);
                this.stats.totalPacketsProcessed++;

                // Enforce absolute maximum buffer size for whichever buffer we're using
                const currentBufferSize = targetBuffer.length;
                if (currentBufferSize >= this.config.maxBufferSize) {
                    Log.w(this.TAG, `Target buffer reached maximum size (${currentBufferSize} packets), forcing dispatch`);
                    
                    // Force dispatch only if we're working with the active buffer and not in transition
                    if (!this._bufferState.transitionPending || targetBuffer === this._bufferState.activeBuffer) {
                        // During drain mode, use the drain logic
                        if (this._bufferState.drainActive) {
                            // Immediate call to dispatch logic
                            this._dispatchKeyframeAwareChunk();
                        } else {
                            // Normal dispatch for active buffer when not in transition
                            // We can't dispatch from pending buffer yet
                            const packetsToDispatch = Math.min(300, currentBufferSize - this.config.bufferSizeInPackets);
                            Log.v(this.TAG, `Large buffer (${currentBufferSize} packets). Dispatching ${packetsToDispatch} packets.`);
                            this._dispatchPacketChunk(packetsToDispatch);
                        }
                    } else {
                        // If we're in transition and the pending buffer is getting too full, 
                        // just log it - we'll handle it when we switch buffers
                        Log.v(this.TAG, `Pending buffer growing large (${currentBufferSize} packets) during transition.`);
                    }
                }
            }
        }
    }

    // Update buffer fullness stats based on which buffer is currently in use
    const currentBufferSize = this._packetBuffer.length;
    this.stats.bufferFullness = currentBufferSize / this.config.bufferSizeInPackets;

    // Check if initial buffering is complete
    if (!this._initialBufferingComplete) {
        // Only start playback when buffer is filled to the specified threshold
        // AND we have at least one keyframe in the buffer
        if (this.stats.bufferFullness >= this.config.initialBufferingThreshold &&
            (!this.config.keyframeAwareChunking || this._keyframePositions.length > 0)) {

            this._initialBufferingComplete = true;
            this._status = LoaderStatus.kComplete;
            Log.v(this.TAG, `Initial buffering complete: ${Math.round(this.stats.bufferFullness * 100)}% full with ${this._keyframePositions.length} keyframes`);
        } else {
            // Still in initial buffering phase
            return; // Don't dispatch packets yet
        }
    }

    // Regular dispatch processing follows the usual rules except in transition mode
    // In transition mode, _dispatchKeyframeAwareChunk() manages everything
}

/**
 * Update internal references to point to the active buffer
 * This is a helper function to ensure all operations use the correct buffer
 */
_useActiveBuffer() {
    if (this._bufferState) {
        this._packetBuffer = this._bufferState.activeBuffer;
        this._keyframePositions = this._bufferState.activeKeyframePositions;
    }
}

/**
 * Update internal references to point to the pending buffer
 * This is a helper function to ensure all operations use the correct buffer
 */
_usePendingBuffer() {
    if (this._bufferState) {
        this._packetBuffer = this._bufferState.pendingBuffer;
        this._keyframePositions = this._bufferState.pendingKeyframePositions;
    }
}

/**
 * Log diagnostics information about the stream, buffer and PTS continuity
 * Enhanced to include dual buffer information
 */
_logDiagnostics() {
    if (this._requestAbort) return;

    try {
        Log.v(this.TAG, "========= MPEG-TS STREAM DIAGNOSTIC REPORT =========");
        Log.v(this.TAG, `WebTransport State: ${this._transport ? (this._transport.closed ? 'closed' : 'open') : 'null'}`);
        Log.v(this.TAG, `Total Bytes Received: ${this._receivedLength}`);
        Log.v(this.TAG, `Total Packets Processed: ${this.stats.totalPacketsProcessed}`);
        Log.v(this.TAG, `Streams Received: ${this.stats.streamsReceived}`);

        // Enhanced buffer logging with dual buffer information
        if (this._bufferState && this._bufferState.transitionPending) {
            Log.v(this.TAG, `Dual Buffer Mode: ACTIVE (stream transition in progress)`);
            Log.v(this.TAG, `Current Stream: #${this._bufferState.streamCounter}`);
            Log.v(this.TAG, `Active Buffer Size: ${this._bufferState.activeBuffer.length} packets`);
            Log.v(this.TAG, `Pending Buffer Size: ${this._bufferState.pendingBuffer.length} packets`);
            Log.v(this.TAG, `Draining Active Buffer: ${this._bufferState.drainActive ? 'YES' : 'NO'}`);
        } else {
            Log.v(this.TAG, `Dual Buffer Mode: INACTIVE (single stream operation)`);
        }

        // Standard buffer logging
        Log.v(this.TAG, `Buffer Configuration:`);
        Log.v(this.TAG, `  Target Size: ${this.config.bufferSizeInPackets} packets (${(this.config.bufferSizeInPackets * this.PACKET_SIZE / 1024).toFixed(1)} KB)`);
        Log.v(this.TAG, `  Dispatch Chunk Size: ${this.config.dispatchChunkSize} packets`);
        Log.v(this.TAG, `  Initial Buffering Threshold: ${this.config.initialBufferingThreshold * 100}%`);
        Log.v(this.TAG, `  Keyframe-Aware Chunking: ${this.config.keyframeAwareChunking ? 'ENABLED' : 'DISABLED'}`);

        Log.v(this.TAG, `Buffer Status:`);
        const currentBuffer = this._packetBuffer || [];
        Log.v(this.TAG, `  Current Size: ${currentBuffer.length} packets (${(currentBuffer.length * this.PACKET_SIZE / 1024).toFixed(1)} KB)`);
        Log.v(this.TAG, `  Fullness: ${Math.round(this.stats.bufferFullness * 100)}%`);
        Log.v(this.TAG, `  Initial Buffering: ${this._initialBufferingComplete ? 'Complete' : 'In Progress'}`);

        // Calculate and log estimated buffer duration
        const bufferSizeBytes = currentBuffer.length * this.PACKET_SIZE;
        // Assuming typical bitrate of 5Mbps video + 128Kbps audio
        const typicalBitrate = 5 * 1024 * 1024 + 128 * 1024; // bits per second
        const typicalByterate = typicalBitrate / 8; // bytes per second
        const estimatedDurationMs = (bufferSizeBytes / typicalByterate) * 1000;
        Log.v(this.TAG, `Estimated Buffer Duration: ${Math.round(estimatedDurationMs)}ms`);

        // Enhanced keyframe tracking with dual buffer awareness
        const keyframes = this._keyframePositions || [];
        Log.v(this.TAG, `Keyframe Tracking:`);
        Log.v(this.TAG, `  Keyframes Detected: ${this.stats.keyframesDetected}`);
        Log.v(this.TAG, `  Keyframes in Current Buffer: ${keyframes.length}`);

        if (keyframes.length > 0) {
            Log.v(this.TAG, `  First Keyframe Position: ${keyframes[0]}`);

            // Calculate time between keyframes if we have multiple
            if (keyframes.length > 1) {
                let totalDistanceBetweenKeyframes = 0;
                let keyframeIntervals = keyframes.length - 1;

                for (let i = 0; i < keyframes.length - 1; i++) {
                    totalDistanceBetweenKeyframes += (keyframes[i+1] - keyframes[i]);
                }

                const avgPacketsBetweenKeyframes = totalDistanceBetweenKeyframes / keyframeIntervals;
                const avgBytesBetweenKeyframes = avgPacketsBetweenKeyframes * this.PACKET_SIZE;
                const avgTimeBetweenKeyframes = (avgBytesBetweenKeyframes / typicalByterate) * 1000;

                Log.v(this.TAG, `  Avg Keyframe Interval: ~${Math.round(avgTimeBetweenKeyframes)}ms (${Math.round(avgPacketsBetweenKeyframes)} packets)`);
            }

            // Log all keyframe positions for debugging if there are few of them
            if (keyframes.length <= 10) {
                Log.v(this.TAG, `  All Keyframe Positions: ${keyframes.join(', ')}`);
            } else {
                // Just show first few and last few
                const firstFew = keyframes.slice(0, 3).join(', ');
                const lastFew = keyframes.slice(-3).join(', ');
                Log.v(this.TAG, `  Keyframe Positions Sample: ${firstFew}, ... , ${lastFew}`);
            }
        }

        if (this.config.enablePTSContinuity) {
            Log.v(this.TAG, `PTS Continuity: ENABLED`);
            Log.v(this.TAG, `Last Video PTS: ${this.stats.lastPTS.video !== null ? this.stats.lastPTS.video : 'None'}`);
            Log.v(this.TAG, `Last Audio PTS: ${this.stats.lastPTS.audio !== null ? this.stats.lastPTS.audio : 'None'}`);
            const unifiedAdjustment = this.stats.ptsAdjustments.video;
            Log.v(this.TAG, `Unified PTS Adjustment: ${unifiedAdjustment} (applied to all streams)`);
        } else {
            Log.v(this.TAG, `PTS Continuity: DISABLED`);
        }

        Log.v(this.TAG, "====================================================");
    } catch (e) {
        Log.e(this.TAG, `Error in diagnostics: ${e.message}`);
    }
}

    /**
     * Significantly enhanced keyframe detection that is more aggressive about finding keyframes
     * This is especially important for stream transitions
     */
    _rescanBufferForKeyframes() {
        // Reset keyframe tracking
        this._keyframePositions = [];
        this._lastDispatchedKeyframeIndex = -1;

        // Keep track of detection methods that worked
        const successfulMethods = [];
        let totalKeyframesFound = 0;

        // Try method 1: RAI detection
        let keyframesFoundRAI = 0;
        for (let i = 0; i < this._packetBuffer.length; i++) {
            if (this._isKeyframeByRAI(this._packetBuffer[i])) {
                this._keyframePositions.push(i);
                keyframesFoundRAI++;
            }
        }
        
        if (keyframesFoundRAI > 0) {
            successfulMethods.push('RAI');
            totalKeyframesFound = keyframesFoundRAI;
            if (this.config.enableDetailedLogging) {
                Log.v(this.TAG, `Rescan found ${keyframesFoundRAI} keyframes using RAI method`);
            }
        }

        // If RAI method didn't find enough keyframes, try NAL detection
        if (totalKeyframesFound < 2) {
            this._keyframePositions = [];
            let keyframesFoundNAL = 0;
            
            for (let i = 0; i < this._packetBuffer.length; i++) {
                if (this._isKeyframeByNAL(this._packetBuffer[i])) {
                    this._keyframePositions.push(i);
                    keyframesFoundNAL++;
                }
            }
            
            if (keyframesFoundNAL > 0) {
                successfulMethods.push('NAL');
                totalKeyframesFound = keyframesFoundNAL;
                if (this.config.enableDetailedLogging) {
                    Log.v(this.TAG, `Rescan found ${keyframesFoundNAL} keyframes using NAL method`);
                }
            }
        }

        // If NAL method didn't find enough keyframes, try PES header detection
        if (totalKeyframesFound < 2) {
            this._keyframePositions = [];
            let keyframesFoundPES = 0;
            
            for (let i = 0; i < this._packetBuffer.length; i++) {
                if (this._isKeyframeByPESHeader(this._packetBuffer[i])) {
                    this._keyframePositions.push(i);
                    keyframesFoundPES++;
                }
            }
            
            if (keyframesFoundPES > 0) {
                successfulMethods.push('PES');
                totalKeyframesFound = keyframesFoundPES;
                if (this.config.enableDetailedLogging) {
                    Log.v(this.TAG, `Rescan found ${keyframesFoundPES} keyframes using PES method`);
                }
            }
        }

        // Final approach: look for packet patterns that often indicate keyframes
        if (totalKeyframesFound < 2) {
            this._keyframePositions = [];
            let keyframesFoundPattern = 0;
            
            // Scan for patterns that might indicate keyframes:
            // 1. Look for adaptation field with PCR flag set
            // 2. Packets with large adaptation fields
            // 3. Look for PES packet starts with specific stream IDs
            
            const scanIncrement = Math.max(1, Math.floor(this._packetBuffer.length / 1000)); // Optimization for large buffers
            
            for (let i = 0; i < this._packetBuffer.length; i += scanIncrement) {
                const packet = this._packetBuffer[i];
                
                // Basic check for TS packet
                if (packet[0] !== 0x47) continue;
                
                // Check for payload unit start indicator (PUSI)
                const hasPUSI = (packet[1] & 0x40) !== 0;
                
                // Check adaptation field control bits
                const adaptationFieldControl = (packet[3] & 0x30) >> 4;
                const hasAdaptationField = (adaptationFieldControl & 0x2) !== 0;
                const hasPayload = (adaptationFieldControl & 0x1) !== 0;
                
                // Skip packets without adaptation field or payload
                if (!hasAdaptationField && !hasPayload) continue;
                
                let isLikelyKeyframe = false;
                
                // Check adaptation field for PCR flag if present
                if (hasAdaptationField && packet.length > 5) {
                    const adaptationFieldLength = packet[4];
                    
                    if (adaptationFieldLength > 0 && packet.length > 5) {
                        // Large adaptation fields often indicate keyframes
                        if (adaptationFieldLength > 30) {
                            isLikelyKeyframe = true;
                        }
                        
                        // Check for PCR flag - often present in keyframes
                        if ((packet[5] & 0x10) !== 0) {
                            isLikelyKeyframe = true;
                        }
                    }
                }
                
                // Check for PES packet start
                if (hasPUSI && hasPayload) {
                    let payloadOffset = 4;
                    if (hasAdaptationField) {
                        payloadOffset = 5 + (packet[4] || 0);
                    }
                    
                    // Ensure we have enough data
                    if (payloadOffset + 9 < packet.length) {
                        // Check for PES start code (0x00 0x00 0x01)
                        if (packet[payloadOffset] === 0 && 
                            packet[payloadOffset + 1] === 0 && 
                            packet[payloadOffset + 2] === 1) {
                            
                            // Video PES packets often indicate scene changes
                            const streamId = packet[payloadOffset + 3];
                            if (streamId >= 0xE0 && streamId <= 0xEF) {
                                isLikelyKeyframe = true;
                            }
                        }
                    }
                }
                
                if (isLikelyKeyframe) {
                    this._keyframePositions.push(i);
                    keyframesFoundPattern++;
                }
            }
            
            if (keyframesFoundPattern > 0) {
                successfulMethods.push('Pattern');
                totalKeyframesFound = keyframesFoundPattern;
                if (this.config.enableDetailedLogging) {
                    Log.v(this.TAG, `Rescan found ${keyframesFoundPattern} keyframes using pattern heuristics`);
                }
            }
        }

        // Additional heuristic: Look for packets with specific payload patterns
        if (totalKeyframesFound < 2) {
            this._keyframePositions = [];
            let keyframesFoundHeuristic = 0;
            
            // Sample the buffer at regular intervals to detect potential scene changes
            const bufferSize = this._packetBuffer.length;
            const samplingRate = Math.max(1, Math.floor(bufferSize / 20)); // Check ~20 positions
            
            for (let i = 0; i < bufferSize; i += samplingRate) {
                // Add first packet as a keyframe
                if (i === 0) {
                    this._keyframePositions.push(0);
                    keyframesFoundHeuristic++;
                    continue;
                }
                
                // Add last position if we're near the end
                if (i > bufferSize - samplingRate && i < bufferSize - 10) {
                    this._keyframePositions.push(i);
                    keyframesFoundHeuristic++;
                    continue;
                }
                
                // For middle positions, add some keyframes
                if (keyframesFoundHeuristic < 5) {
                    this._keyframePositions.push(i);
                    keyframesFoundHeuristic++;
                }
            }
            
            if (keyframesFoundHeuristic > 0) {
                successfulMethods.push('Heuristic');
                totalKeyframesFound = keyframesFoundHeuristic;
                if (this.config.enableDetailedLogging) {
                    Log.v(this.TAG, `Added ${keyframesFoundHeuristic} keyframe positions using heuristic sampling`);
                }
            }
        }

        // If we found keyframes, log the results
        if (totalKeyframesFound > 0) {
            Log.v(this.TAG, `Rescan found ${totalKeyframesFound} keyframes using methods: ${successfulMethods.join(', ')}`);
            this.stats.keyframesDetected = totalKeyframesFound;
            
            // Log specific positions if there are few keyframes
            if (this._keyframePositions.length <= 10) {
                for (let i = 0; i < this._keyframePositions.length; i++) {
                    Log.v(this.TAG, `Rescan found keyframe at position ${this._keyframePositions[i]}`);
                }
            }
        } else {
            // Fallback: If we still can't find keyframes, add artificial ones
            Log.w(this.TAG, `Failed to detect any keyframes during rescan. Adding artificial keyframe markers.`);

            // Add artificial keyframes at regular intervals
            const bufferSize = this._packetBuffer.length;
            
            // First packet is always a keyframe
            this._keyframePositions.push(0);
            
            // Add more keyframes at regular intervals
            const keyframeInterval = Math.floor(bufferSize / Math.min(5, bufferSize / 50));
            for (let i = keyframeInterval; i < bufferSize; i += keyframeInterval) {
                this._keyframePositions.push(i);
            }

            Log.v(this.TAG, `Added ${this._keyframePositions.length} artificial keyframe markers`);
            this.stats.keyframesDetected = this._keyframePositions.length;
        }
        
        // Remove any duplicate positions and sort
        this._keyframePositions = [...new Set(this._keyframePositions)].sort((a, b) => a - b);
        
        // Always make sure at least the first packet is marked as a keyframe
        if (this._keyframePositions.length === 0 && this._packetBuffer.length > 0) {
            this._keyframePositions.push(0);
        } else if (this._keyframePositions.length > 0 && this._keyframePositions[0] > 0) {
            this._keyframePositions.unshift(0);
        }
    }

    /**
     * Check for keyframes using the Random Access Indicator in adaptation field
     */
    _isKeyframeByRAI(packet) {
        try {
            // Skip packets that don't start with sync byte
            if (packet[0] !== 0x47) {
                return false;
            }

            // Check if adaptation field exists
            const adaptationFieldControl = (packet[3] & 0x30) >> 4;
            const hasAdaptationField = (adaptationFieldControl & 0x2) !== 0;

            if (hasAdaptationField && packet.length > 5 && packet[4] > 0) {
                // Check random access indicator in adaptation field flags
                const randomAccessIndicator = (packet[5] & 0x40) !== 0;
                return randomAccessIndicator;
            }

            return false;
        } catch (e) {
            return false;
        }
    }

    /**
     * Check for keyframes by scanning for H.264 NAL units with IDR picture type
     */
    _isKeyframeByNAL(packet) {
        try {
            // Similar to original method but with more thorough scanning
            if (packet[0] !== 0x47) {
                return false;
            }

            // Get payload offset
            const adaptationFieldControl = (packet[3] & 0x30) >> 4;
            const hasAdaptationField = (adaptationFieldControl & 0x2) !== 0;

            if ((adaptationFieldControl & 0x1) === 0) {
                return false; // No payload
            }

            let payloadOffset = 4;
            if (hasAdaptationField && packet.length > 4) {
                const adaptationFieldLength = packet[4];
                payloadOffset = 5 + adaptationFieldLength;
            }

            // Scan entire payload for NAL unit signatures
            for (let i = payloadOffset; i < packet.length - 5; i++) {
                // Look for NAL start codes
                if ((packet[i] === 0 && packet[i + 1] === 0 && packet[i + 2] === 1) ||
                    (i < packet.length - 6 && packet[i] === 0 && packet[i + 1] === 0 && packet[i + 2] === 0 && packet[i + 3] === 1)) {

                    const nalStartOffset = (packet[i + 2] === 1) ? i + 3 : i + 4;
                    if (nalStartOffset < packet.length) {
                        const nalType = packet[nalStartOffset] & 0x1F;
                        if (nalType === 5) { // IDR picture
                            return true;
                        }
                    }
                }
            }

            return false;
        } catch (e) {
            return false;
        }
    }

/**
     * Check for keyframes by examining PES packet headers for specific flags
     */
    _isKeyframeByPESHeader(packet) {
        try {
            // Only packets with PUSI (Payload Unit Start Indicator) can contain the start of a keyframe
            if (packet[0] !== 0x47 || (packet[1] & 0x40) === 0) {
                return false;
            }

            // Get payload offset
            const adaptationFieldControl = (packet[3] & 0x30) >> 4;
            const hasAdaptationField = (adaptationFieldControl & 0x2) !== 0;

            if ((adaptationFieldControl & 0x1) === 0) {
                return false; // No payload
            }

            let payloadOffset = 4;
            if (hasAdaptationField && packet.length > 4) {
                const adaptationFieldLength = packet[4];
                payloadOffset = 5 + adaptationFieldLength;
            }

            // Check for PES start code
            if (payloadOffset + 9 > packet.length ||
                packet[payloadOffset] !== 0 ||
                packet[payloadOffset + 1] !== 0 ||
                packet[payloadOffset + 2] !== 1) {
                return false;
            }

            // Check if this is a video stream
            const streamId = packet[payloadOffset + 3];
            if (streamId < 0xE0 || streamId > 0xEF) {
                return false; // Not a video stream
            }

            // Check if this PES contains a picture start code
            // We need to check the PES extension data
            if (payloadOffset + 9 > packet.length) {
                return false;
            }

            const pesHeaderDataLength = packet[payloadOffset + 8];
            const headerEnd = payloadOffset + 9 + pesHeaderDataLength;

            if (headerEnd + 4 < packet.length) {
                // Look for picture start code (00 00 01 00) after PES header
                if (packet[headerEnd] === 0 &&
                    packet[headerEnd + 1] === 0 &&
                    packet[headerEnd + 2] === 1 &&
                    packet[headerEnd + 3] === 0) {

                    // This is likely a picture start code, which often indicates keyframe
                    return true;
                }
            }

            return false;
        } catch (e) {
            return false;
        }
    }

    /**
     * Determines if a packet contains a keyframe (IDR frame)
     *
     * @param {Uint8Array} packet MPEG-TS packet
     * @returns {boolean} True if packet contains a keyframe
     */
    _isKeyframePacket(packet) {
        try {
            // Skip packets that don't start with sync byte
            if (packet[0] !== 0x47) {
                return false;
            }

            // Extract PID (Packet Identifier)
            const pid = ((packet[1] & 0x1F) << 8) | packet[2];

            // Check if this is a video PES packet
            // This is a simplification - in production you'd match against known video PIDs from PMT
            const isVideoPES = pid >= 0x1E0 && pid <= 0x1EF;
            if (!isVideoPES) {
                return false;
            }

            // Check for payload unit start indicator
            const payloadStartIndicator = (packet[1] & 0x40) !== 0;
            if (!payloadStartIndicator) {
                return false; // Only packets starting a PES can contain keyframe headers
            }

            // Check adaptation field and get payload offset
            const adaptationFieldControl = (packet[3] & 0x30) >> 4;
            if (adaptationFieldControl === 0 || adaptationFieldControl === 2) {
                return false; // No payload
            }

            const hasAdaptationField = (adaptationFieldControl & 0x2) !== 0;
            let payloadOffset = 4;

            if (hasAdaptationField) {
                const adaptationFieldLength = packet[4];
                if (adaptationFieldLength > 0) {
                    // Check random access indicator in adaptation field
                    // This is set for packets containing keyframes
                    const randomAccessIndicator = (packet[5] & 0x40) !== 0;
                    if (randomAccessIndicator) {
                        if (this.config.enableDetailedLogging) {
                            Log.v(this.TAG, `Keyframe detected via random access indicator`);
                        }
                        return true;
                    }
                }
                payloadOffset = 5 + adaptationFieldLength;
            }

            // If we reach here, we need to check the PES payload for H.264 NAL units
            if (payloadOffset >= packet.length - 9) {
                return false; // Not enough data for PES header
            }

            // Check for PES start code (0x00 0x00 0x01)
            if (packet[payloadOffset] !== 0 ||
                packet[payloadOffset + 1] !== 0 ||
                packet[payloadOffset + 2] !== 1) {
                return false;
            }

            // Check if this is a video stream ID
            const streamId = packet[payloadOffset + 3];
            if (streamId < 0xE0 || streamId > 0xEF) {
                return false;
            }

            // Skip PES header to find the H.264 data
            // PES header length is at payloadOffset + 8
            const pesHeaderLength = packet[payloadOffset + 8];
            const nalOffset = payloadOffset + 9 + pesHeaderLength;

            // Look for NAL units in the remaining bytes
            for (let i = nalOffset; i < packet.length - 4; i++) {
                // Look for NAL start code
                if ((packet[i] === 0 && packet[i + 1] === 0 && packet[i + 2] === 1) ||
                    (packet[i] === 0 && packet[i + 1] === 0 && packet[i + 2] === 0 && packet[i + 3] === 1)) {

                    // Skip start code to get NAL type
                    const startCodeLength = (packet[i + 2] === 1) ? 3 : 4;
                    const nalTypeOffset = i + startCodeLength;

                    if (nalTypeOffset < packet.length) {
                        // Extract NAL unit type (5 bits from the first byte)
                        const nalType = packet[nalTypeOffset] & 0x1F;

                        // NAL type 5 = IDR picture (keyframe in H.264)
                        if (nalType === 5) {
                            if (this.config.enableDetailedLogging) {
                                Log.v(this.TAG, `Keyframe detected via NAL type 5 (IDR)`);
                            }
                            return true;
                        }
                    }
                }
            }

            return false;
        } catch (e) {
            Log.w(this.TAG, `Error detecting keyframe: ${e.message}`);
            return false;
        }
    }

    async abort() {
        this._requestAbort = true;

        // Clear diagnostic timer
        if (this.diagnosticTimer) {
            clearInterval(this.diagnosticTimer);
            this.diagnosticTimer = null;
        }

        try {
            // Cancel current stream reader if active
            if (this._currentStreamReader) {
                await this._currentStreamReader.cancel("Loader aborted").catch(() => {});
                this._currentStreamReader = null;
            }

            // Cancel stream reader
            if (this._streamReader) {
                await this._streamReader.cancel("Loader aborted").catch(() => {});
                this._streamReader = null;
            }

            // Dispatch any remaining packets
            if (this._packetBuffer.length > 0) {
                this._dispatchPacketChunk(this._packetBuffer.length);
            }

            // Close transport
            if (this._transport && !this._transport.closed) {
                await this._transport.close().catch(() => {});
            }

        } catch (e) {
            Log.e(this.TAG, `Error during abort: ${e.message}`);
        } finally {
            this._transport = null;
            this._packetBuffer = [];
            this._status = LoaderStatus.kComplete;
        }
    }

    /**
     * Log diagnostics information about the stream, buffer and PTS continuity
     */
    _logDiagnostics() {
        if (this._requestAbort) return;

        try {
            Log.v(this.TAG, "========= MPEG-TS STREAM DIAGNOSTIC REPORT =========");
            Log.v(this.TAG, `WebTransport State: ${this._transport ? (this._transport.closed ? 'closed' : 'open') : 'null'}`);
            Log.v(this.TAG, `Total Bytes Received: ${this._receivedLength}`);
            Log.v(this.TAG, `Total Packets Processed: ${this.stats.totalPacketsProcessed}`);
            Log.v(this.TAG, `Streams Received: ${this.stats.streamsReceived}`);

            // Enhanced buffer logging
            Log.v(this.TAG, `Buffer Configuration:`);
            Log.v(this.TAG, `  Target Size: ${this.config.bufferSizeInPackets} packets (${(this.config.bufferSizeInPackets * this.PACKET_SIZE / 1024).toFixed(1)} KB)`);
            Log.v(this.TAG, `  Dispatch Chunk Size: ${this.config.dispatchChunkSize} packets`);
            Log.v(this.TAG, `  Initial Buffering Threshold: ${this.config.initialBufferingThreshold * 100}%`);
            Log.v(this.TAG, `  Keyframe-Aware Chunking: ${this.config.keyframeAwareChunking ? 'ENABLED' : 'DISABLED'}`);

            Log.v(this.TAG, `Buffer Status:`);
            Log.v(this.TAG, `  Current Size: ${this._packetBuffer.length} packets (${(this._packetBuffer.length * this.PACKET_SIZE / 1024).toFixed(1)} KB)`);
            Log.v(this.TAG, `  Fullness: ${Math.round(this.stats.bufferFullness * 100)}%`);
            Log.v(this.TAG, `  Initial Buffering: ${this._initialBufferingComplete ? 'Complete' : 'In Progress'}`);

            // Calculate and log estimated buffer duration
            const bufferSizeBytes = this._packetBuffer.length * this.PACKET_SIZE;
            // Assuming typical bitrate of 5Mbps video + 128Kbps audio
            const typicalBitrate = 5 * 1024 * 1024 + 128 * 1024; // bits per second
            const typicalByterate = typicalBitrate / 8; // bytes per second
            const estimatedDurationMs = (bufferSizeBytes / typicalByterate) * 1000;
            Log.v(this.TAG, `Estimated Buffer Duration: ${Math.round(estimatedDurationMs)}ms`);

            if (this.config.keyframeAwareChunking) {
                Log.v(this.TAG, `Keyframe Tracking:`);
                Log.v(this.TAG, `  Keyframes Detected: ${this.stats.keyframesDetected}`);
                Log.v(this.TAG, `  Keyframes in Buffer: ${this._keyframePositions.length}`);
                Log.v(this.TAG, `  Last Dispatched Keyframe: ${this._lastDispatchedKeyframeIndex}`);

                if (this._keyframePositions.length > 0) {
                    Log.v(this.TAG, `  First Keyframe Position: ${this._keyframePositions[0]}`);

                    // Calculate time between keyframes if we have multiple
                    if (this._keyframePositions.length > 1) {
                        let totalDistanceBetweenKeyframes = 0;
                        let keyframeIntervals = this._keyframePositions.length - 1;

                        for (let i = 0; i < this._keyframePositions.length - 1; i++) {
                            totalDistanceBetweenKeyframes += (this._keyframePositions[i+1] - this._keyframePositions[i]);
                        }

                        const avgPacketsBetweenKeyframes = totalDistanceBetweenKeyframes / keyframeIntervals;
                        const avgBytesBetweenKeyframes = avgPacketsBetweenKeyframes * this.PACKET_SIZE;
                        const avgTimeBetweenKeyframes = (avgBytesBetweenKeyframes / typicalByterate) * 1000;

                        Log.v(this.TAG, `  Avg Keyframe Interval: ~${Math.round(avgTimeBetweenKeyframes)}ms (${Math.round(avgPacketsBetweenKeyframes)} packets)`);
                    }

                    // Log all keyframe positions for debugging if there are few of them
                    if (this._keyframePositions.length <= 10) {
                        Log.v(this.TAG, `  All Keyframe Positions: ${this._keyframePositions.join(', ')}`);
                    } else {
                        // Just show first and last few
                        const firstThree = this._keyframePositions.slice(0, 3).join(', ');
                        const lastThree = this._keyframePositions.slice(-3).join(', ');
                        Log.v(this.TAG, `  Keyframe Positions Sample: ${firstThree}, ... , ${lastThree}`);
                    }
                }
            }

            if (this.config.enablePTSContinuity) {
                Log.v(this.TAG, `PTS Continuity: ENABLED`);
                Log.v(this.TAG, `Last Video PTS: ${this.stats.lastPTS.video !== null ? this.stats.lastPTS.video : 'None'}`);
                Log.v(this.TAG, `Last Audio PTS: ${this.stats.lastPTS.audio !== null ? this.stats.lastPTS.audio : 'None'}`);
                const unifiedAdjustment = this.stats.ptsAdjustments.video;
                Log.v(this.TAG, `Unified PTS Adjustment: ${unifiedAdjustment} (applied to all streams)`);
            } else {
                Log.v(this.TAG, `PTS Continuity: DISABLED`);
            }

            Log.v(this.TAG, "====================================================");
        } catch (e) {
            Log.e(this.TAG, `Error in diagnostics: ${e.message}`);
        }
    }

    /**
     * Reset timestamp continuity tracking
     * This should be called during seek operations
     */
    resetPTSContinuity() {
        if (this.config.enablePTSContinuity && this._ptsHandler) {
            this._ptsHandler.reset();
            Log.v(this.TAG, "PTS continuity tracking has been reset");

            // Reset stats
            this.stats.lastPTS = {
                video: null,
                audio: null
            };
            this.stats.ptsAdjustments = {
                video: 0,
                audio: 0
            };
        }

        // Reset keyframe tracking
        this._keyframePositions = [];
        this._lastDispatchedKeyframeIndex = -1;
        this.stats.keyframesDetected = 0;
    }
}

export default WebTransportLoader;
