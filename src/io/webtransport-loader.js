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

// For MPEG-TS/FLV over WebTransport live stream
class WebTransportLoader extends BaseLoader {

    static isSupported() {
        try {
            return (typeof self.WebTransport !== 'undefined');
        } catch (e) {
            return false;
        }
    }

    constructor() {
        super('webtransport-loader');
        this.TAG = 'WebTransportLoader';

        this._needStash = true;

        this._transport = null;
        this._reader = null;
        this._requestAbort = false;
        this._receivedLength = 0;
    }

    destroy() {
        if (this._transport) {
            this.abort();
        }
        super.destroy();
    }

    async open(dataSource) {
        try {
            // Create WebTransport instance
            const transport = this._transport = new WebTransport(dataSource.url);
            
            // Wait for connection to be established
            await transport.ready;
            
            // Create a unidirectional stream for receiving data
            const incomingStream = await transport.createUnidirectionalStream();
            this._reader = incomingStream.getReader();
            
            this._status = LoaderStatus.kBuffering;
            
            // Start reading data
            this._readChunks();
            
        } catch (e) {
            this._status = LoaderStatus.kError;

            const info = {code: e.code || -1, msg: e.message};

            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, info);
            } else {
                throw new RuntimeException(info.msg);
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

    async _readChunks() {
        try {
            while (true) {
                const {value, done} = await this._reader.read();
                
                if (done || this._requestAbort) {
                    break;
                }

                // value is a Uint8Array
                if (value) {
                    this._dispatchArrayBuffer(value.buffer);
                }
            }

            if (!this._requestAbort) {
                this._status = LoaderStatus.kComplete;
                if (this._onComplete) {
                    this._onComplete(0, this._receivedLength - 1);
                }
            }
            
            this._requestAbort = false;
            
        } catch (e) {
            this._status = LoaderStatus.kError;
            const info = {
                code: e.code || -1,
                msg: e.message
            };

            if (this._onError) {
                this._onError(LoaderErrors.EXCEPTION, info);
            } else {
                throw new RuntimeException(info.msg);
            }
        }
    }

    _dispatchArrayBuffer(arraybuffer) {
        const chunk = arraybuffer;
        const byteStart = this._receivedLength;
        this._receivedLength += chunk.byteLength;

        if (this._onDataArrival) {
            this._onDataArrival(chunk, byteStart, this._receivedLength);
        }
    }
}

export default WebTransportLoader;
