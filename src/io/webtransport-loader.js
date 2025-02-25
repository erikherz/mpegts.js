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

class MPEGTSTestHarness {
    constructor(options = {}) {
        this.onLog = options.onLog || console.log;
        this.TAG = 'MPEGTSTestHarness';
        this.packetSize = 188;
        this.validationResults = {
            totalChunks: 0,
            validChunks: 0,
            totalPackets: 0,
            validPackets: 0,
            pidStats: new Map(),
            pesPackets: 0,
            validPTS: 0,
            continuityErrors: 0,
            alignmentErrors: 0,
            structureErrors: 0
        };

        // Track continuity counters for each PID
        this.continuityCounters = new Map();
    }

    reset() {
        this.validationResults = {
            totalChunks: 0,
            validChunks: 0,
            totalPackets: 0,
            validPackets: 0,
            pidStats: new Map(),
            pesPackets: 0,
            validPTS: 0,
            continuityErrors: 0,
            alignmentErrors: 0,
            structureErrors: 0
        };
        this.continuityCounters = new Map();
    }

    // Process and validate a chunk of data
    validateChunk(chunk) {
        if (!chunk || !(chunk instanceof ArrayBuffer || chunk instanceof Uint8Array)) {
            this.onLog(`${this.TAG}: Invalid chunk type`);
            return false;
        }

        const data = chunk instanceof ArrayBuffer ? new Uint8Array(chunk) : chunk;
        this.validationResults.totalChunks++;

        // Check if size is multiple of packet size
        if (data.length % this.packetSize !== 0) {
            this.onLog(`${this.TAG}: Chunk size (${data.length}) is not a multiple of ${this.packetSize}`);
            this.validationResults.alignmentErrors++;
            return false;
        }

        // Process each packet in the chunk
        let allPacketsValid = true;
        const packets = data.length / this.packetSize;

        for (let i = 0; i < packets; i++) {
            const packetOffset = i * this.packetSize;
            const packetValid = this.validatePacket(data, packetOffset);

            if (!packetValid) {
                allPacketsValid = false;
            }
        }

        if (allPacketsValid) {
            this.validationResults.validChunks++;
        }

        return allPacketsValid;
    }

    // Validate a single MPEG-TS packet
    validatePacket(data, offset) {
        this.validationResults.totalPackets++;

        // 1. Check sync byte
        if (data[offset] !== 0x47) {
            this.onLog(`${this.TAG}: Invalid sync byte at offset ${offset}: 0x${data[offset].toString(16)}`);
            return false;
        }

        // 2. Extract basic packet information
        const transportError = (data[offset + 1] & 0x80) !== 0;
        const payloadStart = (data[offset + 1] & 0x40) !== 0;
        const pid = ((data[offset + 1] & 0x1F) << 8) | data[offset + 2];
        const scramblingControl = (data[offset + 3] & 0xC0) >> 6;
        const hasAdaptation = (data[offset + 3] & 0x20) !== 0;
        const hasPayload = (data[offset + 3] & 0x10) !== 0;
        const continuityCounter = data[offset + 3] & 0x0F;

        // Update PID statistics
        this.validationResults.pidStats.set(pid,
            (this.validationResults.pidStats.get(pid) || 0) + 1);

        // 3. Check for transport errors
        if (transportError) {
            this.onLog(`${this.TAG}: Transport error flag set at offset ${offset} for PID 0x${pid.toString(16)}`);
            this.validationResults.structureErrors++;
            return false;
        }

        // 4. Validate continuity counter
        if (this.continuityCounters.has(pid)) {
            const expectedCC = (this.continuityCounters.get(pid) + 1) & 0x0F;
            if (continuityCounter !== expectedCC) {
                this.onLog(`${this.TAG}: Continuity counter error for PID 0x${pid.toString(16)}: ` +
                    `expected ${expectedCC}, got ${continuityCounter}`);
                this.validationResults.continuityErrors++;
            }
        }
        this.continuityCounters.set(pid, continuityCounter);

        // 5. Validate adaptation field if present
        if (hasAdaptation) {
            const adaptationLength = data[offset + 4];

            if (adaptationLength > 183) {
                this.onLog(`${this.TAG}: Invalid adaptation field length ${adaptationLength} at offset ${offset}`);
                this.validationResults.structureErrors++;
                return false;
            }
        }

        // 6. Check payload start indicator and PES header if applicable
        if (payloadStart && hasPayload) {
            this.validationResults.pesPackets++;

            const payloadOffset = hasAdaptation ? offset + 5 + data[offset + 4] : offset + 4;

            // Validate PES header if we have enough data
            if (payloadOffset + 9 <= offset + this.packetSize) {
                if (data[payloadOffset] === 0x00 &&
                    data[payloadOffset + 1] === 0x00 &&
                    data[payloadOffset + 2] === 0x01) {

                    // Check for PTS
                    if (payloadOffset + 14 <= offset + this.packetSize) {
                        const ptsDtsFlags = (data[payloadOffset + 7] & 0xC0) >> 6;

                        if (ptsDtsFlags > 0) {
                            // Validate PTS marker bits
                            const p0 = data[payloadOffset + 9];
                            if ((p0 & 0xF0) >> 4 !== 0x2 && (p0 & 0xF0) >> 4 !== 0x3) {
                                this.onLog(`${this.TAG}: Invalid PTS marker bits: 0x${p0.toString(16)}`);
                            } else {
                                this.validationResults.validPTS++;
                            }
                        }
                    }
                }
            }
        }

        // 7. All checks passed
        this.validationResults.validPackets++;
        return true;
    }

    // Generate a human-readable report
    generateReport() {
        const summary = `
============ MPEG-TS Test Harness Report ============
Total chunks processed: ${this.validationResults.totalChunks}
Valid chunks: ${this.validationResults.validChunks} (${(this.validationResults.validChunks / this.validationResults.totalChunks * 100).toFixed(1)}%)
Total packets: ${this.validationResults.totalPackets}
Valid packets: ${this.validationResults.validPackets} (${(this.validationResults.validPackets / this.validationResults.totalPackets * 100).toFixed(1)}%)
PES packets: ${this.validationResults.pesPackets}
Valid PTS values: ${this.validationResults.validPTS}
Continuity errors: ${this.validationResults.continuityErrors}
Alignment errors: ${this.validationResults.alignmentErrors}
Structure errors: ${this.validationResults.structureErrors}

PID distribution:`;

        let pidInfo = '';
        for (const [pid, count] of this.validationResults.pidStats.entries()) {
            pidInfo += `\n  PID 0x${pid.toString(16).padStart(4, '0')}: ${count} packets (${(count / this.validationResults.totalPackets * 100).toFixed(1)}%)`;

            // Add interpretation for common PIDs
            if (pid === 0x0000) {
                pidInfo += " - Program Association Table (PAT)";
            } else if (pid === 0x0001) {
                pidInfo += " - Conditional Access Table (CAT)";
            } else if (pid === 0x1FFF) {
                pidInfo += " - Null packets";
            } else if (pid === 0x1000 || pid === 0x0100 || pid === 0x4096) {
                pidInfo += " - Likely Program Map Table (PMT)";
            } else if (pid === 256 || pid === 0x1100 || pid === 0x0041) {
                pidInfo += " - Video elementary stream";
            } else if (pid === 257 || pid === 0x1200 || pid === 0x0043) {
                pidInfo += " - Audio elementary stream";
            }
        }

        // Add assessment of stream quality
        let assessment = "\nOverall assessment: ";

        if (this.validationResults.validPackets === 0) {
            assessment += "CRITICAL - No valid packets found";
        } else if (this.validationResults.validPackets / this.validationResults.totalPackets < 0.8) {
            assessment += "POOR - Less than 80% of packets are valid";
        } else if (this.validationResults.structureErrors > 0 || this.validationResults.alignmentErrors > 0) {
            assessment += "PROBLEMATIC - Contains structural errors";
        } else if (this.validationResults.continuityErrors > 0) {
            assessment += "FAIR - Contains continuity errors but basic structure is valid";
        } else if (!this.validationResults.pidStats.has(0x0000)) {
            assessment += "INCOMPLETE - Missing Program Association Table (PAT)";
        } else if (this.validationResults.validPTS === 0) {
            assessment += "PROBLEMATIC - No valid PTS values found";
        } else {
            assessment += "GOOD - Stream appears to be well-formed";
        }

        // Add recommendations based on findings
        let recommendations = "\n\nRecommendations:";

        if (this.validationResults.alignmentErrors > 0) {
            recommendations += "\n- Fix packet alignment by ensuring chunks are multiples of 188 bytes";
            recommendations += "\n- Ensure sync bytes (0x47) occur at the start of each 188-byte packet";
        }

        if (this.validationResults.structureErrors > 0) {
            recommendations += "\n- Check adaptation field lengths and packet structure";
            recommendations += "\n- Verify PES header formation, especially for packets with payload_unit_start_indicator set";
        }

        if (this.validationResults.continuityErrors > 0) {
            recommendations += "\n- Investigate packet loss or reordering in the transport layer";
            recommendations += "\n- Ensure packets for each PID maintain proper continuity counter sequence";
        }

        if (!this.validationResults.pidStats.has(0x0000)) {
            recommendations += "\n- Ensure PAT packets are included in the stream";
        }

        if (this.validationResults.validPTS === 0 && this.validationResults.pesPackets > 0) {
            recommendations += "\n- Check PTS extraction logic and PES header formation";
        }

        return summary + pidInfo + assessment + recommendations;
    }

    // Integrate with your WebTransportLoader class
    integrateWithLoader(loader) {
        const originalDispatchMethod = loader._dispatchPacketChunk.bind(loader);

        // Override the dispatch method to add validation
        loader._dispatchPacketChunk = () => {
            if (loader._packetBuffer.length === 0) return;

            // Combine packets into a single chunk
            const totalLength = loader._packetBuffer.reduce((sum, packet) => sum + packet.length, 0);
            const chunk = new Uint8Array(totalLength);

            let offset = 0;
            loader._packetBuffer.forEach(packet => {
                chunk.set(packet, offset);
                offset += packet.length;
            });

            // Validate the chunk
            const isValid = this.validateChunk(chunk.buffer);

            if (!isValid) {
                loader.onLog(`${this.TAG}: Invalid chunk detected, validation report follows:`);
                loader.onLog(this.generateReport());
            }

            // Call the original method
            originalDispatchMethod();
        };

	    if (typeof this.onLog === 'function') {
		this.onLog(`${this.TAG}: Successfully integrated with WebTransportLoader`);
	    } else if (typeof loader.onLog === 'function') {
		loader.onLog(`${this.TAG}: Successfully integrated with WebTransportLoader`);
	    } else {
		console.log(`${this.TAG}: Successfully integrated with WebTransportLoader`);
	    }

        return true;
    }
}

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
        //this.onLog(`[RAW] Bytes at ${start}: ${bytes}`);
    }

    logVideoPacketDetails(buffer, index, payloadStart, hasAdaptationField, continuityCounter) {
        const payloadOffset = hasAdaptationField ? 5 + buffer[index + 4] : 4;

        //this.onLog(`[DEBUG] Video packet at ${index}: START=${payloadStart}, AF=${hasAdaptationField}, CC=${continuityCounter}`);

        if (payloadStart && buffer.length >= index + payloadOffset + 4) {
            const startCode = buffer.slice(index + payloadOffset, index + payloadOffset + 4);
            if (startCode[0] === 0x00 && startCode[1] === 0x00 &&
                startCode[2] === 0x00 && startCode[3] === 0x01) {
                //this.onLog(`[DEBUG] Valid H.264 start code found at offset ${payloadOffset}`);
            }
        }
    }
}

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

        // Log incoming chunk details
        //this.onLog(`Processing new chunk of ${inputChunk.length} bytes`);

        // Append new data to existing buffer
        let newBuffer = new Uint8Array(this.buffer.length + inputChunk.length);
        newBuffer.set(this.buffer, 0);
        newBuffer.set(inputChunk, this.buffer.length);
        this.buffer = newBuffer;

        // Log buffer state
        //this.onLog(`Total buffer size after append: ${this.buffer.length} bytes`);

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
			//this.onLog(`[WARNING] Incomplete packet at end of buffer: ${packets.length - i} bytes`);
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
		    
		    // Special handling for video packets - check for PID 256 (0x100)
		    // Also check the usual video PIDs as a fallback
		    if ((pid === 0x100 || pid === 0x0041 || pid === 0xE0) && hasPayload) {
			const payloadOffset = hasAdaptationField ? 5 + currentPacket[4] : 4;
			
			if (payloadStart && currentPacket.length >= payloadOffset + 9) {
			    // Check for PES start code
			    if (currentPacket[payloadOffset] === 0x00 && 
				currentPacket[payloadOffset + 1] === 0x00 &&
				currentPacket[payloadOffset + 2] === 0x01) {
				
				const streamID = currentPacket[payloadOffset + 3];
				const ptsDtsFlags = (currentPacket[payloadOffset + 7] & 0xC0) >> 6;
				
				// Check for PTS
				if (ptsDtsFlags > 0 && currentPacket.length >= payloadOffset + 14) {
				    const ptsBytes = Array.from(currentPacket.slice(payloadOffset + 9, payloadOffset + 14))
					.map(b => b.toString(16).padStart(2, '0'))
					.join(' ');
				    //this.onLog(`[DEBUG] PTS bytes for PID ${pid.toString(16)}: ${ptsBytes}`);
				}
			    }
			}
		    }
		    
		    validPackets.push(currentPacket);
		}

		// Log periodic statistics
		if (this.stats.totalPacketsProcessed % 1000 === 0) {
		    try {
			if (typeof this.logStats === 'function') {
			    //this.logStats();
			} else {
			    // Fallback minimal logging
			    // this.onLog(`MPEGTS Buffer Stats: Total=${this.stats.totalPacketsProcessed}, Valid=${this.stats.validPackets}`);
			}
		    } catch (e) {
			this.onLog(`[ERROR] Error logging stats: ${e.message}`);
		    }
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
			//this.onLog(`[INFO] Found ${validSyncCount} consecutive sync bytes at position ${i}`);
			return i;
		    }
		}
	    }
	    
	    // Fallback: return the first sync byte we find, even if it doesn't have follow-ups
	    for (let i = 0; i <= buffer.length - this.PACKET_SIZE; i++) {
		if (buffer[i] === this.SYNC_BYTE) {
		    //this.onLog(`[INFO] Found single sync byte at position ${i} (no follow-ups)`);
		    return i;
		}
	    }
	    
	    return -1;
	}

}

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

	debugPTS(rawBytes) {
	    // Input is array of 5 bytes, as extracted from PES header
	    if (!Array.isArray(rawBytes) || rawBytes.length !== 5) {
		this.onLog(`[ERROR] Invalid PTS bytes array: ${rawBytes}`);
		return;
	    }
	    
	    const [p0, p1, p2, p3, p4] = rawBytes;
	    
		/*
	    this.onLog(`
	PTS Breakdown:
	=============
	Raw bytes: ${rawBytes.map(b => '0x' + b.toString(16).padStart(2, '0')).join(', ')}

	Byte 1 (0x${p0.toString(16).padStart(2, '0')}): ${p0.toString(2).padStart(8, '0')}
	   Should start with 0010 (prefix) and end with 1 (marker bit)
	   Actual: starts with ${(p0 >> 4).toString(2).padStart(4, '0')}, ends with ${p0 & 0x01}
	   Bits 32-30 of PTS: ${((p0 >> 1) & 0x07).toString(2).padStart(3, '0')}

	Byte 2 (0x${p1.toString(16).padStart(2, '0')}): ${p1.toString(2).padStart(8, '0')}
	   Contains bits 29-22 of PTS

	Byte 3 (0x${p2.toString(16).padStart(2, '0')}): ${p2.toString(2).padStart(8, '0')}
	   Should end with 1 (marker bit)
	   Actual: ends with ${p2 & 0x01}
	   Bits 21-15 of PTS: ${((p2 >> 1) & 0x7F).toString(2).padStart(7, '0')}

	Byte 4 (0x${p3.toString(16).padStart(2, '0')}): ${p3.toString(2).padStart(8, '0')}
	   Contains bits 14-7 of PTS

	Byte 5 (0x${p4.toString(16).padStart(2, '0')}): ${p4.toString(2).padStart(8, '0')}
	   Should end with 1 (marker bit)
	   Actual: ends with ${p4 & 0x01}
	   Bits 6-0 of PTS: ${((p4 >> 1) & 0x7F).toString(2).padStart(7, '0')}

	Calculated PTS value:
	${(((p0 >> 1) & 0x07) << 30) | 
	  (p1 << 22) | 
	  (((p2 >> 1) & 0x7F) << 15) | 
	  (p3 << 7) | 
	  ((p4 >> 1) & 0x7F)}

	Standard calculation (ignoring marker bits):
	${(((p0 >> 1) & 0x07) << 30) | 
	  (p1 << 22) | 
	  (((p2 >> 1) & 0x7F) << 15) | 
	  (p3 << 7) | 
	  ((p4 >> 1) & 0x7F)}

	Alternative calculation (forcing marker bits):
	${(((p0 >> 1) & 0x07) << 30) | 
	  (p1 << 22) | 
	  (((p2 | 0x01) >> 1) << 15) | 
	  (p3 << 7) | 
	  (((p4 | 0x01) >> 1) & 0x7F)}
	`);
*/
	    return (((p0 >> 1) & 0x07) << 30) | 
		   (p1 << 22) | 
		   (((p2 >> 1) & 0x7F) << 15) | 
		   (p3 << 7) | 
		   ((p4 >> 1) & 0x7F);
	}

    _formatBits(byte) {
        return byte.toString(2).padStart(8, '0');
    }

	_detectPTSDiscontinuity(pts) {
	    if (this.prevPTS !== null) {
		const diff = pts - this.prevPTS;
		// Check for large gaps or backwards jumps
		if (Math.abs(diff) > 900000) { // 10-second gap
		    Log.w(this.TAG, `Large PTS discontinuity detected: ${diff / 90000} seconds`);
		    return true;
		}
	    }
	    this.prevPTS = pts;
	    return false;
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

	if (this._checkForH264IFrame(payload, payloadOffset)) {
	    //Log.v(this.TAG, `I-frame detected at PTS ${this.lastValidPTS}`);
	    // Track keyframes for debugging
	    if (!this.keyframes) this.keyframes = [];
	    this.keyframes.push({
		pts: this.lastValidPTS,
		packetCount: this.packetCount,
		timeReceived: Date.now()
	    });
	}

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
			
			if (randomAccessIndicator === 1) {
			    //this.onLog(`[INFO] Random Access Indicator found at packet #${this.packetCount}`);
			}
		    }
		    
		    if (adaptationFieldLength > 183) return;
		    payloadOffset = 5 + adaptationFieldLength;
		}

		if (payloadOffset >= packet.length) return;

		if (payloadUnitStart) {
		    this.debugStats.pesStarts++;
		    const payload = packet.slice(payloadOffset);
		    
		    // Debug full PES header details
		    this._debugPESHeaderDetailed(payload, randomAccessIndicator);
		    
		    // Check for H.264 NAL unit that might indicate an I-frame
		    this._checkForH264IFrame(payload, payloadOffset);
		    
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
			
			//this.onLog(`[INFO] Found PTS: ${pts} in video packet #${this.debugStats.videoPIDPackets}`);
		    }
		}
	    }

	    if (this.packetCount === 1 || this.packetCount === 100 || 
		this.packetCount === 1000 || this.packetCount % 1000 === 0) {
		//this._logDetailedStats(timeReceived);
	    }
	}

	_checkForH264IFrame(payload, payloadOffset) {
	    try {
		// First ensure payload is defined
		if (!payload || !payload.length) {
		    return false;
		}
		
		// Check if we have the start of a PES packet
		if (payload.length < 9) {
		    return false;
		}
		
		if (payload[0] !== 0x00 || payload[1] !== 0x00 || payload[2] !== 0x01) {
		    return false;
		}
		
		// Get to the actual video data after PES header
		const pesHeaderLength = payload[8];
		if (pesHeaderLength === undefined || 
		    payload.length < 9 + pesHeaderLength + 5) {
		    return false;
		}
		
		// Move to video data (after PES header)
		const videoData = payload.slice(9 + pesHeaderLength);
		if (!videoData || !videoData.length) {
		    return false;
		}
		
		// Look for NAL units (start with 0x000001 or 0x00000001)
		for (let i = 0; i < videoData.length - 5; i++) {
		    // Check for NAL unit start code
		    if ((videoData[i] === 0 && videoData[i+1] === 0 && videoData[i+2] === 1) ||
			(videoData[i] === 0 && videoData[i+1] === 0 && videoData[i+2] === 0 && videoData[i+3] === 1)) {
			
			// Get NAL type - 5 is IDR (I-frame)
			const startCodeSize = (videoData[i+2] === 1) ? 3 : 4;
			
			// Make sure we have enough data for the NAL type
			if (i + startCodeSize >= videoData.length) {
			    continue;
			}
			
			const nalType = videoData[i + startCodeSize] & 0x1F;
			
			// NAL type 5 = IDR (I-frame)
			if (nalType === 5) {
			    return true;
			}
		    }
		}
		
		return false;
	    } catch (e) {
		Log.e(this.TAG, `Error checking for I-frame: ${e.message}`);
		return false;
	    }
	}

	_debugPESHeaderDetailed(payload, randomAccessIndicator) {
	    if (payload.length < 19) return;

	    if (payload[0] === 0x00 && payload[1] === 0x00 && payload[2] === 0x01) {
		const streamId = payload[3];
		const pesPacketLength = (payload[4] << 8) | payload[5];
		const scramblingControl = (payload[6] & 0x30) >> 4;
		const priority = (payload[6] & 0x08) >> 3;
		const dataAlignmentIndicator = (payload[6] & 0x04) >> 2;
		const copyright = (payload[6] & 0x02) >> 1;
		const originalOrCopy = payload[6] & 0x01;
		const ptsDtsFlags = (payload[7] & 0xC0) >> 6;
		const escrFlag = (payload[7] & 0x20) >> 5;
		const esRateFlag = (payload[7] & 0x10) >> 4;
		const dsmTrickModeFlag = (payload[7] & 0x08) >> 3;
		const additionalCopyInfoFlag = (payload[7] & 0x04) >> 2;
		const pesCrcFlag = (payload[7] & 0x02) >> 1;
		const pesExtensionFlag = payload[7] & 0x01;
		const pesHeaderLength = payload[8];

		    /*
		this.onLog(`PES Header Detailed Debug:
		    Start Code: ${payload[0]},${payload[1]},${payload[2]}
		    Stream ID: 0x${streamId.toString(16)}
		    Packet Length: ${pesPacketLength}
		    Scrambling Control: ${scramblingControl}
		    Priority: ${priority}
		    Data Alignment: ${dataAlignmentIndicator}
		    Copyright: ${copyright}
		    Original/Copy: ${originalOrCopy}
		    PTS_DTS_flags: ${ptsDtsFlags}
		    ESCR flag: ${escrFlag}
		    ES Rate flag: ${esRateFlag}
		    DSM Trick Mode: ${dsmTrickModeFlag}
		    Additional Copy Info: ${additionalCopyInfoFlag}
		    PES CRC flag: ${pesCrcFlag}
		    PES Extension: ${pesExtensionFlag}
		    PES Header Length: ${pesHeaderLength}
		    Random Access Indicator: ${randomAccessIndicator}
		    Raw PTS bytes: ${payload.slice(9, 14).map(b => b.toString(16).padStart(2, '0')).join(' ')}
		    PTS byte details:
			Byte 1 (0x${payload[9].toString(16).padStart(2, '0')}): ${this._formatBits(payload[9])}
			Byte 2 (0x${payload[10].toString(16).padStart(2, '0')}): ${this._formatBits(payload[10])}
			Byte 3 (0x${payload[11].toString(16).padStart(2, '0')}): ${this._formatBits(payload[11])}
			Byte 4 (0x${payload[12].toString(16).padStart(2, '0')}): ${this._formatBits(payload[12])}
			Byte 5 (0x${payload[13].toString(16).padStart(2, '0')}): ${this._formatBits(payload[13])}`);

		     */

		// Return an object with parsed header information
		return {
		    streamId,
		    pesPacketLength,
		    randomAccessIndicator,
		    ptsDtsFlags,
		    pesHeaderLength,
		    // Add other fields as needed
		};
	    }
	    return null;
	}

	logDetailedStatsConditional(timeReceived, payload) {
	    // Only log for the 1st, 100th, 1000th, and every 1000th packet thereafter.
	    if (this.packetCount === 1 || this.packetCount === 100 || this.packetCount % 1000 === 0) {
		// Build Packet Stats message.
		const pts = (this.lastValidPTS !== null)
		    ? this.lastValidPTS
		    : (this.packetCount * this.estimatedFrameDuration);
		let statsMsg = `[WebTransportLoader] > Packet Stats #${this.packetCount}:\n`;
		statsMsg += `            PTS: ${pts}\n`;
		statsMsg += `            Received at: ${timeReceived}\n`;
		statsMsg += `            Total Packets: ${this.debugStats.totalPackets}\n`;
		statsMsg += `            Video PID (256) Packets: ${this.debugStats.videoPIDPackets}\n`;
		statsMsg += `            PES Packet Starts: ${this.debugStats.pesStarts}\n`;
		statsMsg += `            Valid PTS Found: ${this.debugStats.validPTS}\n`;
		statsMsg += `            Not PES Header: ${this.debugStats.notPESHeader}\n`;
		statsMsg += `            No PTS: ${this.debugStats.noPTS}\n`;
		statsMsg += `            Last 5 PTS values: ${this.debugStats.pts.join(', ')}\n`;
		//this.onLog(statsMsg);

		// Log PES Header Detailed Debug if the payload is available.
		if (payload && payload.length >= 14) {
		    let headerMsg = `[WebTransportLoader] > PES Header Detailed Debug:\n`;
		    headerMsg += `                Start Code: ${payload[0]},${payload[1]},${payload[2]}\n`;
		    headerMsg += `                Stream ID: 0x${payload[3].toString(16)}\n`;
		    headerMsg += `                Packet Length: ${(payload[4] << 8) | payload[5]}\n`;
		    headerMsg += `                Scrambling Control: ${(payload[6] & 0x30) >> 4}\n`;
		    headerMsg += `                Priority: ${(payload[6] & 0x08) >> 3}\n`;
		    headerMsg += `                Data Alignment: ${(payload[6] & 0x04) >> 2}\n`;
		    headerMsg += `                Copyright: ${(payload[6] & 0x02) >> 1}\n`;
		    headerMsg += `                Original/Copy: ${payload[6] & 0x01}\n`;
		    headerMsg += `                PTS_DTS_flags: ${(payload[7] & 0xC0) >> 6}\n`;
		    headerMsg += `                ESCR flag: ${(payload[7] & 0x20) >> 5}\n`;
		    headerMsg += `                ES Rate flag: ${(payload[7] & 0x10) >> 4}\n`;
		    headerMsg += `                DSM Trick Mode: ${(payload[7] & 0x08) >> 3}\n`;
		    headerMsg += `                Additional Copy Info: ${(payload[7] & 0x04) >> 2}\n`;
		    headerMsg += `                PES CRC flag: ${(payload[7] & 0x02) >> 1}\n`;
		    headerMsg += `                PES Extension: ${payload[7] & 0x01}\n`;
		    headerMsg += `                PES Header Length: ${payload[8]}\n`;
		    headerMsg += `                Raw PTS bytes: ${payload.slice(9, 14).map(b => b.toString(16).padStart(2, '0')).join(' ')}\n`;
		    headerMsg += `                PTS byte details:\n`;
		    headerMsg += `                    Byte 1 (0x${payload[9].toString(16).padStart(2, '0')}): ${payload[9].toString(2).padStart(8, '0')}\n`;
		    headerMsg += `                    Byte 2 (0x${payload[10].toString(16).padStart(2, '0')}): ${payload[10].toString(2).padStart(8, '0')}\n`;
		    headerMsg += `                    Byte 3 (0x${payload[11].toString(16).padStart(2, '0')}): ${payload[11].toString(2).padStart(8, '0')}\n`;
		    headerMsg += `                    Byte 4 (0x${payload[12].toString(16).padStart(2, '0')}): ${payload[12].toString(2).padStart(8, '0')}\n`;
		    headerMsg += `                    Byte 5 (0x${payload[13].toString(16).padStart(2, '0')}): ${payload[13].toString(2).padStart(8, '0')}\n`;
		    //this.onLog(headerMsg);
		}
	    }
	}

	_extractPTS(payload) {
	    // Log detailed stats and PES header info (conditional)
	    //this.logDetailedStatsConditional(Date.now(), payload);

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
	    const p0 = payload[9];   // e.g. 0x21
	    const p1 = payload[10];  // e.g. 0x00
	    const p2 = payload[11];  // e.g. 0x03
	    const p3 = payload[12];  // e.g. 0x35
	    const p4 = payload[13];  // e.g. 0x61

	    // Log raw PTS bytes for debugging
	    const hexBytes = [p0, p1, p2, p3, p4].map(b => b.toString(16).padStart(2, '0')).join(' ');
	    //this.onLog(`[DEBUG] Raw PTS bytes: ${hexBytes}`);
	    
	    // Extract the 33-bit PTS, handling marker bits correctly
	    const pts =
		(((p0 >> 1) & 0x07) << 30) | // Bits 32-30
		(p1 << 22) |                 // Bits 29-22
		(((p2 >> 1) & 0x7F) << 15) | // Bits 21-15
		(p3 << 7) |                  // Bits 14-7
		((p4 >> 1) & 0x7F);          // Bits 6-0

	    // Log the computed PTS value
	    //this.onLog(`[INFO] Extracted PTS: ${pts} from bytes: ${hexBytes}`);
	    
	    // Update statistics
	    this.debugStats.validPTS++;
	    this.lastValidPTS = pts;
	    
	    this.debugStats.pts.push(pts);
	    if (this.debugStats.pts.length > 5) {
		this.debugStats.pts.shift();
	    }

	    return pts;
	}

    _handleWraparound(pts) {
        if (this.lastValidPTS !== null && pts < this.lastValidPTS) {
            const ptsDrop = this.lastValidPTS - pts;
            if (ptsDrop > 4294967296) {
                this.wraparoundOffset += 8589934592;
                //this.onLog(`PTS wraparound detected! New offset: ${this.wraparoundOffset}`);
            }
            pts += this.wraparoundOffset;
        }
        return pts;
    }

    _logDetailedStats(timeReceived) {
        const pts = this.lastValidPTS !== null ? 
                   this.lastValidPTS : 
                   (this.packetCount * this.estimatedFrameDuration);

	    /*
        this.onLog(`Packet Stats #${this.packetCount}:
            PTS: ${pts}
            Received at: ${timeReceived}
            Total Packets: ${this.debugStats.totalPackets}
            Video PID (256) Packets: ${this.debugStats.videoPIDPackets}
            PES Packet Starts: ${this.debugStats.pesStarts}
            Valid PTS Found: ${this.debugStats.validPTS}
            Not PES Header: ${this.debugStats.notPESHeader}
            No PTS: ${this.debugStats.noPTS}
            Last 5 PTS values: ${this.debugStats.pts.join(', ')}`);
	    */
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

        // Initialize with bound logging function
        const logFunction = (msg) => Log.v(this.TAG, msg);
        this.tsBuffer = new MPEGTSBuffer(logFunction);
        this.packetLogger = new PacketLogger(logFunction);

        // Bind methods
        this._readChunks = this._readChunks.bind(this);
        this._processPackets = this._processPackets.bind(this);
	this.PACKETS_PER_CHUNK = 15;  
        this._packetBuffer = [];

	this.testHarness = new MPEGTSTestHarness({
	    onLog: (msg) => { Log.v(this.TAG, msg); }
	});
    }

    static isSupported() {
        try {
            return typeof self.WebTransport !== 'undefined';
        } catch (e) {
            return false;
        }
    }

    destroy() {
        if (this._transport) {
            this.abort();
        }
        super.destroy();
    }

	_handleBufferUnderrun() {
	    Log.w(this.TAG, "Buffer underrun detected, attempting to recover");
	    
	    // Request higher priority for the next few chunks
	    this.priorityMode = true;
	    
	    // Reset any potentially corrupted state
	    this.tsBuffer = new MPEGTSBuffer((msg) => Log.v(this.TAG, msg));
	    this.packetLogger = new PacketLogger((msg) => Log.v(this.TAG, msg));
	    
	    // Notify controller to prepare for discontinuity
	    if (this._onRecoveryNeeded) {
		this._onRecoveryNeeded();
	    }
	}

	async validateTsChunkBeforeSending(chunk) {
	    if (!chunk || chunk.byteLength === 0) {
		Log.w(this.TAG, 'Empty chunk received, skipping validation');
		return false;
	    }

	    const view = new Uint8Array(chunk);
	    const packetSize = 188;
	    
	    // 1. Check if chunk size is a multiple of 188 bytes
	    if (view.byteLength % packetSize !== 0) {
		Log.e(this.TAG, `Chunk size (${view.byteLength}) is not a multiple of ${packetSize}, possible fragmentation`);
		this._logHexDump(view, 0, Math.min(48, view.byteLength), 'Invalid chunk start');
		return false;
	    }
	    
	    // 2. Check if each packet starts with sync byte 0x47
	    let validPackets = 0;
	    let invalidPackets = 0;
	    let pidCounts = new Map();
	    
	    for (let i = 0; i < view.byteLength; i += packetSize) {
		if (view[i] !== 0x47) {
		    invalidPackets++;
		    Log.e(this.TAG, `Invalid sync byte at offset ${i}: 0x${view[i].toString(16)}`);
		    this._logHexDump(view, i, Math.min(16, view.byteLength - i), 'Bad sync');
		    continue;
		}
		
		// Extract PID info
		const pid = ((view[i + 1] & 0x1F) << 8) | view[i + 2];
		pidCounts.set(pid, (pidCounts.get(pid) || 0) + 1);
		
		// Check continuity counter
		const cc = view[i + 3] & 0x0F;
		const hasPayload = (view[i + 3] & 0x10) !== 0;
		const hasAdaptation = (view[i + 3] & 0x20) !== 0;
		
		// Check for adaptation field length validity if present
		if (hasAdaptation) {
		    if (i + 4 >= view.byteLength) {
			Log.e(this.TAG, `Truncated packet at offset ${i}, missing adaptation field length`);
			continue;
		    }
		    
		    const adaptationLength = view[i + 4];
		    if (adaptationLength > 183) {
			Log.e(this.TAG, `Invalid adaptation field length ${adaptationLength} at offset ${i}`);
			continue;
		    }
		}
		
		validPackets++;
	    }
	    
	    // 3. Log validation summary
	    const isValid = invalidPackets === 0 && validPackets > 0;
	    
	    /*
	    Log.i(this.TAG, `Chunk validation: ${isValid ? 'VALID' : 'INVALID'} - ` + 
		  `${validPackets} valid, ${invalidPackets} invalid, ${view.byteLength} bytes total`);
            */
	    
	    // Log PID distribution
	    if (validPackets > 0) {
		let pidInfo = 'PIDs detected: ';
		for (const [pid, count] of pidCounts.entries()) {
		    pidInfo += `0x${pid.toString(16).padStart(4, '0')}(${count}) `;
		}
		//Log.d(this.TAG, pidInfo);
	    }
	    
	    return isValid;
	}

	_logHexDump(data, offset, length, label) {
	    const end = Math.min(offset + length, data.length);
	    const bytes = [];
	    const ascii = [];
	    
	    for (let i = offset; i < end; i++) {
		const byte = data[i];
		bytes.push(byte.toString(16).padStart(2, '0'));
		// Add printable ASCII characters or a dot for non-printable
		ascii.push(byte >= 32 && byte <= 126 ? String.fromCharCode(byte) : '.');
	    }
	    
	    //Log.d(this.TAG, `${label || 'Hex dump'} [${offset}-${end-1}]: ${bytes.join(' ')} | ${ascii.join('')}`);
	}

	_attemptToFixAlignment(chunk) {
	    const view = new Uint8Array(chunk);
	    
	    // Find first sync byte
	    let firstSyncIndex = -1;
	    for (let i = 0; i < Math.min(view.byteLength, 500); i++) {
		if (view[i] === 0x47) {
		    // Check if we have multiple sync bytes at the expected interval
		    let validSyncCount = 1;
		    for (let j = 1; j <= 3; j++) {
			const nextSyncPos = i + (j * 188);
			if (nextSyncPos < view.byteLength && view[nextSyncPos] === 0x47) {
			    validSyncCount++;
			} else {
			    break;
			}
		    }
		    
		    if (validSyncCount >= 2) {
			firstSyncIndex = i;
			break;
		    }
		}
	    }
	    
	    if (firstSyncIndex === -1) {
		Log.e(this.TAG, 'Could not find valid sync pattern in chunk');
		return null;
	    }
	    
	    if (firstSyncIndex === 0) {
		// Alignment is correct
		return chunk;
	    }
	    
	    // Fix alignment by trimming
	    Log.w(this.TAG, `Fixing alignment by trimming ${firstSyncIndex} bytes from start`);
	    return view.slice(firstSyncIndex).buffer;
	}


	async diagnoseStream() {
	    try {
		//Log.v(this.TAG, "Starting stream diagnosis...");
		
		// 1. Check if WebTransport is properly supported
		if (!WebTransportLoader.isSupported()) {
		    Log.e(this.TAG, "WebTransport is not supported in this browser");
		    return false;
		}
		
		// 2. Compare with a known working stream (if available)
		//Log.v(this.TAG, "Suggestion: Try a known working MPEG-TS stream for comparison");
		
		// 3. Check if we're receiving any data
		if (this._receivedLength === 0) {
		    Log.e(this.TAG, "No data received from the WebTransport stream");
		    return false;
		}
		
		// 4. Check if we've found valid TS packets
		if (this.tsBuffer.stats.validPackets === 0) {
		    //Log.e(this.TAG, "No valid TS packets found in the received data");
		    //Log.v(this.TAG, "This could indicate a non-MPEG-TS stream or corrupted data");
		    return false;
		}
		
		// 5. Report on PIDs found
		//Log.v(this.TAG, "PIDs found in the stream:");
		for (const [pid, count] of this.tsBuffer.stats.pidDistribution.entries()) {
		    //Log.v(this.TAG, `  PID 0x${pid.toString(16).padStart(4, '0')}: ${count} packets`);
		}
		
		// 6. Check for expected PIDs
		// Common PIDs: 0x00 (PAT), 0x01 (CAT), 0x1000+ (PMT), 0x1100+ (Video), 0x1200+ (Audio)
		const hasPAT = this.tsBuffer.stats.pidDistribution.has(0);
		const hasVideo = Array.from(this.tsBuffer.stats.pidDistribution.keys())
		    .some(pid => (pid >= 0x1100 && pid <= 0x11FF) || pid === 0x0041 || pid === 0x0042);
		
		if (!hasPAT) {
		    Log.w(this.TAG, "No Program Association Table (PAT) found - stream may be invalid");
		}
		
		if (!hasVideo) {
		    Log.w(this.TAG, "No common video PIDs found - stream may not contain video");
		}
		
		// 7. Check PTS values
		if (this.packetLogger.debugStats.validPTS === 0) {
		    Log.e(this.TAG, "No valid PTS values found - playback might not be possible");
		    return false;
		}
		
		//Log.v(this.TAG, "Stream diagnosis complete - found valid data with PTS values");
		return true;
	    } catch (e) {
		Log.e(this.TAG, `Error during diagnosis: ${e.message}`);
		return false;
	    }
	}

    async open(dataSource) {
	this.diagnoseStream().then(valid => {
	    if (!valid) {
		Log.w(this.TAG, "Stream diagnosis indicates potential issues");
	    }
	});
	this.testHarness.reset();
        this.testHarness.integrateWithLoader(this);
        try {
            if (!dataSource.url.startsWith('https://')) {
                throw new Error('WebTransport requires HTTPS URL');
            }

            Log.v(this.TAG, `Opening WebTransport connection to ${dataSource.url}`);

            this._transport = new WebTransport(dataSource.url);
            await this._transport.ready;

            const incomingStreams = this._transport.incomingUnidirectionalStreams;
            const streamReader = incomingStreams.getReader();
            const { value: stream } = await streamReader.read();

            if (!stream) {
                throw new Error('No incoming stream received');
            }

            this._reader = stream.getReader();
            this._status = LoaderStatus.kBuffering;

            this._readChunks();

            this.diagnosticTimer = setInterval(() => {
               this.advancedStreamDiagnosis();
            }, 10000); // Run diagnosis every 10 seconds

        } catch (e) {
            this._status = LoaderStatus.kError;
            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
            }
        }
    }

	async advancedStreamDiagnosis() {
	    // Create a diagnostic report
	    Log.v(this.TAG, "========= MPEG-TS STREAM DIAGNOSTIC REPORT =========");
	    
	    // 1. WebTransport connection status
	    Log.v(this.TAG, `WebTransport connection: ${this._transport && !this._transport.closed ? 'ACTIVE' : 'INACTIVE'}`);
	    
	    // 2. Received data statistics
	    Log.v(this.TAG, `Total bytes received: ${this._receivedLength}`);
	    
	    // 3. Buffer statistics
	    if (this.tsBuffer) {
		Log.v(this.TAG, `TS Buffer statistics:`);
		Log.v(this.TAG, `  Total packets processed: ${this.tsBuffer.stats.totalPacketsProcessed}`);
		Log.v(this.TAG, `  Valid packets: ${this.tsBuffer.stats.validPackets}`);
		Log.v(this.TAG, `  Invalid packets: ${this.tsBuffer.stats.invalidPackets}`);
		Log.v(this.TAG, `  Invalid sync bytes: ${this.tsBuffer.stats.invalidSyncByteCount}`);
		
		// 4. Detailed PID statistics
		//Log.v(this.TAG, "PID distribution:");
		if (this.tsBuffer.stats.pidDistribution.size === 0) {
		    Log.v(this.TAG, "  No PIDs detected");
		} else {
		    for (const [pid, count] of this.tsBuffer.stats.pidDistribution.entries()) {
			const pidHex = pid.toString(16).padStart(4, '0');
			Log.v(this.TAG, `  PID 0x${pidHex}: ${count} packets (${(count / this.tsBuffer.stats.validPackets * 100).toFixed(1)}%)`);
			
			// Add common PID interpretations
			if (pid === 0x0000) {
			    Log.v(this.TAG, "    - This is the Program Association Table (PAT)");
			} else if (pid === 0x0001) {
			    Log.v(this.TAG, "    - This is the Conditional Access Table (CAT)");
			} else if (pid === 0x1FFF) {
			    Log.v(this.TAG, "    - This is a Null packet");
			} else if (pid >= 0x0002 && pid <= 0x000F) {
			    Log.v(this.TAG, "    - Reserved PID");
			} else if (pid === 0x0100 || pid === 0x1000) {
			    Log.v(this.TAG, "    - Likely Program Map Table (PMT)");
			} else if (pid === 0x0101 || pid === 0x1001) {
			    Log.v(this.TAG, "    - Likely Network Information Table (NIT)");
			} else if (pid >= 0x0100 && pid <= 0x01FF) {
			    Log.v(this.TAG, "    - Likely PMT or other SI/PSI table");
			} else if (pid === 0x0040 || pid === 0x0041 || pid === 0x0042) {
			    Log.v(this.TAG, "    - Standard DVB video PID");
			} else if (pid === 0x0043 || pid === 0x0044 || pid === 0x0045) {
			    Log.v(this.TAG, "    - Standard DVB audio PID");
			} else if ((pid >= 0x1100 && pid <= 0x11FF) || pid === 0x00E0) {
			    Log.v(this.TAG, "    - Likely video elementary stream");
			} else if ((pid >= 0x1200 && pid <= 0x12FF) || (pid >= 0x00C0 && pid <= 0x00DF)) {
			    Log.v(this.TAG, "    - Likely audio elementary stream");
			}
		    }
		}
	    }
	    
	    // 5. PTS statistics
	    if (this.packetLogger) {
		Log.v(this.TAG, "PTS/timing statistics:");
		Log.v(this.TAG, `  Video packets: ${this.packetLogger.debugStats.videoPIDPackets}`);
		Log.v(this.TAG, `  PES packet starts: ${this.packetLogger.debugStats.pesStarts}`);
		Log.v(this.TAG, `  Valid PTS found: ${this.packetLogger.debugStats.validPTS}`);
		
		if (this.packetLogger.debugStats.pts.length > 0) {
		    const ptsValues = this.packetLogger.debugStats.pts;
		    Log.v(this.TAG, `  Last PTS values: ${ptsValues.join(', ')}`);
		    
		    // Check for PTS discontinuities
		    let discontinuities = 0;
		    for (let i = 1; i < ptsValues.length; i++) {
			const diff = ptsValues[i] - ptsValues[i-1];
			if (diff < 0 || diff > 90000) { // More than 1 second gap
			    discontinuities++;
			}
		    }
		    
		    if (discontinuities > 0) {
			Log.v(this.TAG, `  WARNING: ${discontinuities} PTS discontinuities detected`);
		    }
		}
	    }
	    
	    // 6. Attempt to extract and validate program information
	    this._extractProgramInfo();
	    
	    // 7. Detect common MPEG-TS problems
	    this._detectCommonProblems();
	    
	    Log.v(this.TAG, "==================================================");
	}

	// Extract and validate program information from the stream
	_extractProgramInfo() {
	    if (!this.tsBuffer || this.tsBuffer.stats.validPackets === 0) {
		Log.v(this.TAG, "Program information cannot be extracted: No valid packets received");
		return;
	    }
	    
	    Log.v(this.TAG, "Program information analysis:");
	    
	    // Check if we've seen a PAT
	    const hasPAT = this.tsBuffer.stats.pidDistribution.has(0x0000);
	    if (!hasPAT) {
		Log.v(this.TAG, "  WARNING: No Program Association Table (PAT) detected!");
		Log.v(this.TAG, "  - This is critical for proper demuxing");
		return;
	    }
	    
	    // Check for likely PMT PIDs
	    let foundPMT = false;
	    for (const pid of this.tsBuffer.stats.pidDistribution.keys()) {
		// Common PMT PIDs or those referenced in your logs
		if (pid === 0x1000 || pid === 0x0100 || pid === 0x1001 || pid === 0x4096) {
		    Log.v(this.TAG, `  Found potential PMT at PID 0x${pid.toString(16)}`);
		    foundPMT = true;
		}
	    }
	    
	    if (!foundPMT) {
		Log.v(this.TAG, "  WARNING: No Program Map Table (PMT) detected!");
		Log.v(this.TAG, "  - PMT is required to identify audio/video streams");
	    }
	    
	    // Look for video elementary streams
	    let foundVideo = false;
	    for (const pid of this.tsBuffer.stats.pidDistribution.keys()) {
		// Common video PIDs from your code or those in standard ranges
		if (pid === 0x0100 || pid === 0x0041 || pid === 0x00E0 || 
		    pid === 256 || pid === 0x0042 || 
		    (pid >= 0x1100 && pid <= 0x11FF)) {
		    Log.v(this.TAG, `  Found potential video stream at PID 0x${pid.toString(16)}`);
		    foundVideo = true;
		}
	    }
	    
	    if (!foundVideo) {
		Log.v(this.TAG, "  WARNING: No video elementary streams detected!");
	    }
	    
	    // Look for audio elementary streams
	    let foundAudio = false;
	    for (const pid of this.tsBuffer.stats.pidDistribution.keys()) {
		// Common audio PIDs or those in standard ranges
		if (pid === 0x0043 || pid === 0x0044 || pid === 257 ||
		    (pid >= 0x1200 && pid <= 0x12FF) || 
		    (pid >= 0x00C0 && pid <= 0x00DF)) {
		    Log.v(this.TAG, `  Found potential audio stream at PID 0x${pid.toString(16)}`);
		    foundAudio = true;
		}
	    }
	    
	    if (!foundAudio) {
		Log.v(this.TAG, "  WARNING: No audio elementary streams detected!");
	    }
	    
	    // Check based on your error logs' specific PIDs
	    if (this.tsBuffer.stats.pidDistribution.has(256) && 
		this.tsBuffer.stats.pidDistribution.has(257)) {
		Log.v(this.TAG, "  Found PIDs matching your logs: 256 (video h264) and 257 (audio aac)");
	    }
	}

	// Detect common MPEG-TS problems based on gathered statistics
	_detectCommonProblems() {
	    Log.v(this.TAG, "Common MPEG-TS problem detection:");
	    
	    let problems = 0;
	    
	    // 1. Check for sync byte issues
	    if (this.tsBuffer && this.tsBuffer.stats.invalidSyncByteCount > 0) {
		const badSyncPercentage = (this.tsBuffer.stats.invalidSyncByteCount / 
					 (this.tsBuffer.stats.totalPacketsProcessed || 1) * 100).toFixed(2);
		
		Log.v(this.TAG, `  PROBLEM: ${this.tsBuffer.stats.invalidSyncByteCount} packets with invalid sync bytes (${badSyncPercentage}%)`);
		Log.v(this.TAG, "  - This indicates corruption in the transport stream");
		Log.v(this.TAG, "  - Check for network issues or packet loss in WebTransport");
		problems++;
	    }
	    
	    // 2. Check for packet size issues
	    if (this._receivedLength % 188 !== 0) {
		Log.v(this.TAG, `  PROBLEM: Total received bytes (${this._receivedLength}) is not a multiple of 188`);
		Log.v(this.TAG, "  - This suggests packets are being fragmented or not properly aligned");
		Log.v(this.TAG, "  - Ensure packets are aligned to 188-byte boundaries before sending to demuxer");
		problems++;
	    }
	    
	    // 3. Check for missing PAT/PMT
	    if (!this.tsBuffer || !this.tsBuffer.stats.pidDistribution.has(0x0000)) {
		Log.v(this.TAG, "  PROBLEM: Missing Program Association Table (PAT)");
		Log.v(this.TAG, "  - This is crucial for the player to understand the stream structure");
		Log.v(this.TAG, "  - Check if your stream source provides valid PAT");
		problems++;
	    }
	    
	    // 4. Check PES packets vs. PTS findings
	    if (this.packetLogger && 
		this.packetLogger.debugStats.pesStarts > 0 && 
		this.packetLogger.debugStats.validPTS === 0) {
		
		Log.v(this.TAG, "  PROBLEM: Found PES packet starts but no valid PTS values");
		Log.v(this.TAG, "  - This suggests malformed PES headers or timing information");
		Log.v(this.TAG, "  - Check PES header format and PTS extraction logic");
		problems++;
	    }
	    
	    // 5. Check for missing video or audio PIDs based on error logs
	    if (this.tsBuffer && 
		!this.tsBuffer.stats.pidDistribution.has(256) && 
		!this.tsBuffer.stats.pidDistribution.has(0x0100)) {
		
		Log.v(this.TAG, "  PROBLEM: Missing expected video PID (256/0x100)");
		Log.v(this.TAG, "  - The player expects this PID based on your error logs");
		Log.v(this.TAG, "  - Check if your stream uses a different PID for video");
		problems++;
	    }
	    
	    // 6. Check for mismatch between PMT and actual streams
	    // This is a simplified check assuming common PIDs
	    if (this.tsBuffer && 
		this.tsBuffer.stats.pidDistribution.has(0x1000) && 
		!this.tsBuffer.stats.pidDistribution.has(256) && 
		!this.tsBuffer.stats.pidDistribution.has(257)) {
		
		Log.v(this.TAG, "  PROBLEM: PMT may be present but expected elementary streams are missing");
		Log.v(this.TAG, "  - Check that PMT correctly references available elementary streams");
		problems++;
	    }
	    
	    // 7. Check for likely adaptation field issues
	    if (this.tsBuffer && this.tsBuffer.stats.adaptationFieldCount === 0 && 
		this.tsBuffer.stats.validPackets > 100) {
		
		Log.v(this.TAG, "  PROBLEM: No packets with adaptation fields found");
		Log.v(this.TAG, "  - This is unusual for a normal MPEG-TS stream with video");
		Log.v(this.TAG, "  - Could indicate non-standard encoding or stripped timing information");
		problems++;
	    }
	    
	    // 8. Check packet distribution to detect abnormalities
	    if (this.tsBuffer && this.tsBuffer.stats.pidDistribution.size === 1) {
		const pid = Array.from(this.tsBuffer.stats.pidDistribution.keys())[0];
		Log.v(this.TAG, `  PROBLEM: Only one PID (0x${pid.toString(16)}) found in the stream`);
		Log.v(this.TAG, "  - This is extremely unusual for a playable MPEG-TS stream");
		Log.v(this.TAG, "  - Stream may be severely corrupted or not a valid MPEG-TS");
		problems++;
	    }
	    
	    // Summarize findings
	    if (problems === 0) {
		Log.v(this.TAG, "  No common MPEG-TS problems detected");
	    } else {
		Log.v(this.TAG, `  Total problems detected: ${problems}`);
		Log.v(this.TAG, "  RECOMMENDATION: Enable detailed packet validation before sending to demuxer");
	    }
	}

	collectSamplePackets(numPackets = 10) {
	    if (!this._packetBuffer || this._packetBuffer.length === 0) {
		Log.w(this.TAG, "No packets available to sample");
		return null;
	    }
	    
	    const samples = this._packetBuffer.slice(0, Math.min(numPackets, this._packetBuffer.length));
	    
	    Log.v(this.TAG, `Collected ${samples.length} sample packets for analysis`);
	    
	    // Create a detailed analysis of the sampled packets
	    samples.forEach((packet, index) => {
		if (!(packet instanceof Uint8Array) || packet.length !== 188) {
		    Log.e(this.TAG, `Sample #${index}: INVALID PACKET (${packet ? packet.length : 'null'} bytes)`);
		    return;
		}
		
		// Basic packet info
		const syncByte = packet[0];
		const transportError = (packet[1] & 0x80) !== 0;
		const payloadStart = (packet[1] & 0x40) !== 0;
		const pid = ((packet[1] & 0x1F) << 8) | packet[2];
		const hasAdaptation = (packet[3] & 0x20) !== 0;
		const hasPayload = (packet[3] & 0x10) !== 0;
		const continuityCounter = packet[3] & 0x0F;
		
		let info = `Sample #${index}: PID=0x${pid.toString(16).padStart(4, '0')} `;
		info += `CC=${continuityCounter} `;
		info += `Start=${payloadStart ? 'YES' : 'no'} `;
		info += `Error=${transportError ? 'YES' : 'no'} `;
		info += `Adapt=${hasAdaptation ? 'YES' : 'no'} `;
		info += `Payload=${hasPayload ? 'YES' : 'no'}`;
		
		Log.v(this.TAG, info);
		
		// If this is a payload start, try to analyze the PES header
		if (payloadStart && hasPayload) {
		    this._analyzePESHeader(packet, hasAdaptation);
		}
		
		// Log the first 16 bytes in hex for reference
		this._logHexDump(packet, 0, Math.min(16, packet.length), `Sample #${index} header`);
	    });
	    
	    return samples;
	}

	// Helper to analyze PES headers in packets
	_analyzePESHeader(packet, hasAdaptation) {
	    const payloadOffset = hasAdaptation ? 5 + packet[4] : 4;
	    
	    if (packet.length < payloadOffset + 9) {
		Log.v(this.TAG, "  PES: Packet too short for PES header");
		return;
	    }
	    
	    // Check for PES start code
	    if (packet[payloadOffset] !== 0x00 || 
		packet[payloadOffset + 1] !== 0x00 || 
		packet[payloadOffset + 2] !== 0x01) {
		
		Log.v(this.TAG, "  PES: Invalid start code");
		return;
	    }
	    
	    const streamId = packet[payloadOffset + 3];
	    const pesLength = (packet[payloadOffset + 4] << 8) | packet[payloadOffset + 5];
	    
	    let streamType = "Unknown";
	    if (streamId >= 0xE0 && streamId <= 0xEF) {
		streamType = "Video";
	    } else if (streamId >= 0xC0 && streamId <= 0xDF) {
		streamType = "Audio";
	    } else if (streamId === 0xBC) {
		streamType = "Program Stream Map";
	    } else if (streamId === 0xBE) {
		streamType = "Padding Stream";
	    } else if (streamId === 0xBF) {
		streamType = "Private Stream 2";
	    }
	    
	    Log.v(this.TAG, `  PES: ${streamType} stream (ID=0x${streamId.toString(16)}), Length=${pesLength}`);
	    
	    // Check for PTS if we have enough data
	    if (packet.length >= payloadOffset + 14) {
		const ptsDtsFlags = (packet[payloadOffset + 7] & 0xC0) >> 6;
		
		if (ptsDtsFlags > 0) {
		    // Extract and display PTS bytes
		    const ptsBytes = Array.from(packet.slice(payloadOffset + 9, payloadOffset + 14))
			.map(b => b.toString(16).padStart(2, '0'))
			.join(' ');
		    
		    Log.v(this.TAG, `  PES: PTS/DTS flags=${ptsDtsFlags}, PTS bytes=${ptsBytes}`);
		    
		    // Try to calculate actual PTS value
		    try {
			const p0 = packet[payloadOffset + 9];
			const p1 = packet[payloadOffset + 10];
			const p2 = packet[payloadOffset + 11];
			const p3 = packet[payloadOffset + 12];
			const p4 = packet[payloadOffset + 13];
			
			// Check if first bit marker is correct (should be '0010x' where x is a marker bit)
			const prefix = (p0 >> 4) & 0x0F;
			if ((prefix & 0x0E) !== 0x02) {
			    Log.v(this.TAG, `  PES: Invalid PTS prefix (0x${prefix.toString(16)}), should be 0x2 or 0x3`);
			}
			
			// Extract the 33-bit PTS
			const pts = (((p0 >> 1) & 0x07) << 30) | 
				   (p1 << 22) | 
				   (((p2 >> 1) & 0x7F) << 15) | 
				   (p3 << 7) | 
				   ((p4 >> 1) & 0x7F);
			
			Log.v(this.TAG, `  PES: Calculated PTS=${pts} (${(pts/90000).toFixed(3)} seconds)`);
		    } catch (e) {
			Log.v(this.TAG, `  PES: Error calculating PTS: ${e.message}`);
		    }
		} else {
		    Log.v(this.TAG, "  PES: No PTS/DTS flags set");
		}
	    }
	}

    getStreamQualityReport() {
        return this.testHarness.generateReport();
    }

    getStreamHealthReport() {
	    return {
		testHarnessReport: this.testHarness.generateReport(),
		receivedBytes: this._receivedLength,
		packetStats: {
		    total: this.tsBuffer.stats.totalPacketsProcessed,
		    valid: this.tsBuffer.stats.validPackets,
		    invalid: this.tsBuffer.stats.invalidPackets
		},
		ptsStats: {
		    ptsFound: this.packetLogger.debugStats.validPTS,
		    lastPTS: this.packetLogger.lastValidPTS
		}
	    };
    }

    abort() {
	if (this.diagnosticTimer) {
           clearInterval(this.diagnosticTimer);
           this.diagnosticTimer = null;
        }   
        if (this._transport && !this._transport.closed) {
            this._requestAbort = true;
            if (this._reader) {
                this._reader.cancel();
            }
            this._transport.close();
        }
        this._transport = null;
        this._reader = null;
        this._status = LoaderStatus.kComplete;
    }

	_processPackets(packets) {
	    if (!packets || !Array.isArray(packets)) return;
				    
	    const now = Date.now();
	    let validPackets = [];
	    
	    packets.forEach(packet => {
		if (packet instanceof Uint8Array && packet.length === 188) {
		    // Validate individual packet
		    if (packet[0] !== 0x47) {
			Log.e(this.TAG, 'Invalid sync byte in packet, skipping');
			return; // Skip invalid packet
		    }
		    
		    this._receivedLength += packet.length;
		    this.packetLogger.logPacket(packet, now);
		    validPackets.push(packet);
		} else {
		    Log.e(this.TAG, `Invalid packet format or length: ${packet ? packet.length : 'null'}`);
		}
	    });
	    
	    // Track error rate
	    if (validPackets.length === 0 && packets.length > 0) {
		this.consecutiveErrors = (this.consecutiveErrors || 0) + 1;
		if (this.consecutiveErrors > 5) {
		    Log.e(this.TAG, "Too many consecutive errors, attempting recovery");
		    this._handleBufferUnderrun();
		    this.consecutiveErrors = 0;
		}
	    } else if (validPackets.length > 0) {
		this.consecutiveErrors = 0;
	    }
	    
	    // Only add valid packets to the buffer
	    if (validPackets.length > 0) {
		// Update the last packet time for heartbeat checking
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
	    
	    // Combine packets into a single chunk
	    const totalLength = this._packetBuffer.reduce((sum, packet) => sum + packet.length, 0);
	    const chunk = new Uint8Array(totalLength);
	    
	    let offset = 0;
	    this._packetBuffer.forEach(packet => {
		chunk.set(packet, offset);
		offset += packet.length;
	    });

	    // Calculate byte positions
	    const byteStart = this._receivedLength - totalLength;

	    // Validate the chunk before sending
	    const isValid = this.validateTsChunkBeforeSending(chunk.buffer);
	    
	    if (!isValid) {
		Log.e(this.TAG, `Invalid TS chunk detected, may cause demuxer errors. Attempting to fix...`);
		// Try to fix alignment issues
		const fixedChunk = this._attemptToFixAlignment(chunk.buffer);
		if (fixedChunk) {
		    //Log.v(this.TAG, `Sending fixed chunk: ${fixedChunk.byteLength} bytes`);
		    this._onDataArrival(fixedChunk, byteStart, this._receivedLength);
		} else {
		    Log.e(this.TAG, `Could not fix chunk, skipping`);
		}
	    } else {
		// Dispatch the valid chunk
		//Log.v(this.TAG, `Sending valid chunk: ${totalLength} bytes (${this._packetBuffer.length} packets)`);
		this._onDataArrival(chunk.buffer, byteStart, this._receivedLength);
	    }

	    // Clear the buffer
	    this._packetBuffer = [];
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
		let debugCounter = 0;

		while (true) {
		    if (this._requestAbort) break;
		    
		    const { value, done } = await this._reader.read();
		    if (done) {
			//Log.v(this.TAG, `Stream read complete after ${this._receivedLength} bytes`);
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
			let chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
			debugCounter++;
			
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
			    Log.w(this.TAG, `No valid sync pattern found in chunk (${chunk.length} bytes)`);
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
				//Log.v(this.TAG, `Processing ${packets.length} valid packets from ${validLength} bytes`);
				this._processPackets(packets);
			    } else {
				Log.w(this.TAG, `Failed to extract valid packets from ${validLength} bytes`);
			    }
			}
			
			// Store any remaining partial packet for the next chunk
			if (validLength < chunk.length) {
			    pendingData = chunk.slice(validLength);
			    //Log.d(this.TAG, `Storing ${pendingData.length} bytes for next chunk`);
			}
		    }
		}
		
		// Flush any remaining packets
		if (this._packetBuffer.length > 0) {
		    this._dispatchPacketChunk();
		}
		
	    } catch (e) {
		Log.e(this.TAG, `Error in _readChunks: ${e.message}`);
		Log.e(this.TAG, e.stack);
		this._status = LoaderStatus.kError;
		if (this._onError) {
		    this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
		}
	    }
	}
}

export default WebTransportLoader;

