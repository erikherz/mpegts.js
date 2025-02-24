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
        this.onLog(`[RAW] Bytes at ${start}: ${bytes}`);
    }

    logVideoPacketDetails(buffer, index, payloadStart, hasAdaptationField, continuityCounter) {
        const payloadOffset = hasAdaptationField ? 5 + buffer[index + 4] : 4;

        this.onLog(`[DEBUG] Video packet at ${index}: START=${payloadStart}, AF=${hasAdaptationField}, CC=${continuityCounter}`);

        if (payloadStart && buffer.length >= index + payloadOffset + 4) {
            const startCode = buffer.slice(index + payloadOffset, index + payloadOffset + 4);
            if (startCode[0] === 0x00 && startCode[1] === 0x00 &&
                startCode[2] === 0x00 && startCode[3] === 0x01) {
                this.onLog(`[DEBUG] Valid H.264 start code found at offset ${payloadOffset}`);
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
        this.onLog(`Processing new chunk of ${inputChunk.length} bytes`);

        // Append new data to existing buffer
        let newBuffer = new Uint8Array(this.buffer.length + inputChunk.length);
        newBuffer.set(this.buffer, 0);
        newBuffer.set(inputChunk, this.buffer.length);
        this.buffer = newBuffer;

        // Log buffer state
        this.onLog(`Total buffer size after append: ${this.buffer.length} bytes`);

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
			this.onLog(`[WARNING] Incomplete packet at end of buffer: ${packets.length - i} bytes`);
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
				    this.onLog(`[DEBUG] PTS bytes for PID ${pid.toString(16)}: ${ptsBytes}`);
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
			    this.logStats();
			} else {
			    // Fallback minimal logging
			    this.onLog(`MPEGTS Buffer Stats: Total=${this.stats.totalPacketsProcessed}, Valid=${this.stats.validPackets}`);
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
			this.onLog(`[INFO] Found ${validSyncCount} consecutive sync bytes at position ${i}`);
			return i;
		    }
		}
	    }
	    
	    // Fallback: return the first sync byte we find, even if it doesn't have follow-ups
	    for (let i = 0; i <= buffer.length - this.PACKET_SIZE; i++) {
		if (buffer[i] === this.SYNC_BYTE) {
		    this.onLog(`[INFO] Found single sync byte at position ${i} (no follow-ups)`);
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

	    return (((p0 >> 1) & 0x07) << 30) | 
		   (p1 << 22) | 
		   (((p2 >> 1) & 0x7F) << 15) | 
		   (p3 << 7) | 
		   ((p4 >> 1) & 0x7F);
	}

    _formatBits(byte) {
        return byte.toString(2).padStart(8, '0');
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
			
			if (randomAccessIndicator === 1) {
			    this.onLog(`[INFO] Random Access Indicator found at packet #${this.packetCount}`);
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
			
			this.onLog(`[INFO] Found PTS: ${pts} in video packet #${this.debugStats.videoPIDPackets}`);
		    }
		}
	    }

	    if (this.packetCount === 1 || this.packetCount === 100 || 
		this.packetCount === 1000 || this.packetCount % 1000 === 0) {
		this._logDetailedStats(timeReceived);
	    }
	}

	_checkForH264IFrame(payload, payloadOffset) {
	    try {
		// First check if we have the start of a PES packet
		if (payload.length < 9) return false;
		
		if (payload[0] !== 0x00 || payload[1] !== 0x00 || payload[2] !== 0x01) {
		    return false;
		}
		
		// Get to the actual video data after PES header
		const pesHeaderLength = payload[8];
		if (payload.length < 9 + pesHeaderLength + 5) return false;
		
		// Move to video data (after PES header)
		const videoData = payload.slice(9 + pesHeaderLength);
		
		// Look for NAL units (start with 0x000001 or 0x00000001)
		for (let i = 0; i < videoData.length - 5; i++) {
		    // Check for NAL unit start code
		    if ((videoData[i] === 0 && videoData[i+1] === 0 && videoData[i+2] === 1) ||
			(videoData[i] === 0 && videoData[i+1] === 0 && videoData[i+2] === 0 && videoData[i+3] === 1)) {
			
			// Get NAL type - 5 is IDR (I-frame)
			const startCodeSize = (videoData[i+2] === 1) ? 3 : 4;
			const nalType = videoData[i + startCodeSize] & 0x1F;
			
			// NAL type 5 = IDR (I-frame)
			if (nalType === 5) {
			    this.onLog(`[INFO] H.264 I-frame detected at packet #${this.packetCount}`);
			    return true;
			}
		    }
		}
		
		return false;
	    } catch (e) {
		this.onLog(`[ERROR] Error checking for I-frame: ${e.message}`);
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
		this.onLog(statsMsg);

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
		    this.onLog(headerMsg);
		}
	    }
	}

	_extractPTS(payload) {
	    // Log detailed stats and PES header info (conditional)
	    this.logDetailedStatsConditional(Date.now(), payload);

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
	    this.onLog(`[DEBUG] Raw PTS bytes: ${hexBytes}`);
	    
	    // Extract the 33-bit PTS, handling marker bits correctly
	    const pts =
		(((p0 >> 1) & 0x07) << 30) | // Bits 32-30
		(p1 << 22) |                 // Bits 29-22
		(((p2 >> 1) & 0x7F) << 15) | // Bits 21-15
		(p3 << 7) |                  // Bits 14-7
		((p4 >> 1) & 0x7F);          // Bits 6-0

	    // Log the computed PTS value
	    this.onLog(`[INFO] Extracted PTS: ${pts} from bytes: ${hexBytes}`);
	    
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
                this.onLog(`PTS wraparound detected! New offset: ${this.wraparoundOffset}`);
            }
            pts += this.wraparoundOffset;
        }
        return pts;
    }

    _logDetailedStats(timeReceived) {
        const pts = this.lastValidPTS !== null ? 
                   this.lastValidPTS : 
                   (this.packetCount * this.estimatedFrameDuration);

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

        // Initialize with bound logging function
        const logFunction = (msg) => Log.v(this.TAG, msg);
        this.tsBuffer = new MPEGTSBuffer(logFunction);
        this.packetLogger = new PacketLogger(logFunction);

        // Bind methods
        this._readChunks = this._readChunks.bind(this);
        this._processPackets = this._processPackets.bind(this);
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

	async diagnoseStream() {
	    try {
		Log.v(this.TAG, "Starting stream diagnosis...");
		
		// 1. Check if WebTransport is properly supported
		if (!WebTransportLoader.isSupported()) {
		    Log.e(this.TAG, "WebTransport is not supported in this browser");
		    return false;
		}
		
		// 2. Compare with a known working stream (if available)
		Log.v(this.TAG, "Suggestion: Try a known working MPEG-TS stream for comparison");
		
		// 3. Check if we're receiving any data
		if (this._receivedLength === 0) {
		    Log.e(this.TAG, "No data received from the WebTransport stream");
		    return false;
		}
		
		// 4. Check if we've found valid TS packets
		if (this.tsBuffer.stats.validPackets === 0) {
		    Log.e(this.TAG, "No valid TS packets found in the received data");
		    Log.v(this.TAG, "This could indicate a non-MPEG-TS stream or corrupted data");
		    return false;
		}
		
		// 5. Report on PIDs found
		Log.v(this.TAG, "PIDs found in the stream:");
		for (const [pid, count] of this.tsBuffer.stats.pidDistribution.entries()) {
		    Log.v(this.TAG, `  PID 0x${pid.toString(16).padStart(4, '0')}: ${count} packets`);
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
		
		Log.v(this.TAG, "Stream diagnosis complete - found valid data with PTS values");
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

        } catch (e) {
            this._status = LoaderStatus.kError;
            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
            }
        }
    }

    abort() {
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
        packets.forEach(packet => {
            if (packet instanceof Uint8Array) {
                this._receivedLength += packet.length;
                this.packetLogger.logPacket(packet, now);

                if (this._onDataArrival) {
                    //Log.v(this.TAG, `Sending chunk: ${packet.length} bytes at time ${Date.now()}`);
                    this._onDataArrival(packet, this._receivedLength - packet.length, this._receivedLength);
                }
            }
        });
    }

	async _readChunks() {
	    try {
		let fragmentBuffer = new Uint8Array(0);
		this._lastChunkSize = 0;
		let debugCounter = 0;

		while (true) {
		    if (this._requestAbort) break;
		    
		    const { value, done } = await this._reader.read();
		    if (done) {
			Log.v(this.TAG, `Stream read complete after ${this._receivedLength} bytes`);
			break;
		    }

		    if (value) {
			let chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
			
			// Debug log every 20 chunks
			debugCounter++;
			if (debugCounter % 20 === 0 || chunk.byteLength < 100) {
			    Log.v(this.TAG, `Received chunk #${debugCounter}: ${chunk.byteLength} bytes, total: ${this._receivedLength + chunk.byteLength} bytes`);
			    
			    // Log first few bytes for debugging
			    if (chunk.byteLength > 0) {
				const hexBytes = Array.from(chunk.slice(0, Math.min(16, chunk.byteLength)))
				    .map(b => b.toString(16).padStart(2, '0'))
				    .join(' ');
				Log.v(this.TAG, `First bytes: ${hexBytes}`);
			    }
			}

			// Handle fragment from previous chunk
			if (fragmentBuffer.length > 0) {
			    Log.v(this.TAG, `Merging fragment buffer (${fragmentBuffer.length} bytes) with new chunk`);
			    let merged = new Uint8Array(fragmentBuffer.length + chunk.length);
			    merged.set(fragmentBuffer, 0);
			    merged.set(chunk, fragmentBuffer.length);
			    chunk = merged;
			    fragmentBuffer = new Uint8Array(0);
			}

			// Check if we have a sync byte at the start
			if (chunk.length > 0 && chunk[0] !== 0x47) {
			    // Try to find first sync byte
			    let syncIndex = -1;
			    for (let i = 0; i < Math.min(chunk.length, 1000); i++) {
				if (chunk[i] === 0x47) {
				    syncIndex = i;
				    break;
				}
			    }
			    
			    if (syncIndex > 0) {
				Log.v(this.TAG, `Found sync byte at offset ${syncIndex}, trimming chunk`);
				chunk = chunk.slice(syncIndex);
			    } else if (syncIndex === -1) {
				Log.v(this.TAG, `No sync byte found in first 1000 bytes, skipping chunk`);
				continue;
			    }
			}

			const packets = this.tsBuffer.addChunk(chunk);
			if (packets) {
			    Log.v(this.TAG, `Processing ${packets.length} valid packets from chunk`);
			    this._processPackets(packets);
			} else {
			    Log.v(this.TAG, `No valid packets found in chunk of ${chunk.byteLength} bytes`);
			}
		    }
		}
	    } catch (e) {
		Log.e(this.TAG, `Error in _readChunks: ${e.message}`);
		Log.e(this.TAG, e.stack);  // Log the stack trace for more information
		this._status = LoaderStatus.kError;
		if (this._onError) {
		    this._onError(LoaderErrors.EXCEPTION, { code: e.code || -1, msg: e.message });
		}
	    } finally {
		Log.v(this.TAG, `_readChunks ended after processing ${this._receivedLength} bytes`);
	    }
	}
}

export default WebTransportLoader;

