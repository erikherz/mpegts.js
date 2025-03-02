/*
 * Copyright (C) 2016 Bilibili. All Rights Reserved.
 *
 * @author zheng qian <xqq@xqq.im>
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

export const defaultConfig = {
    enableWorker: true,
    enableWorkerForMSE: true,
    enableStashBuffer: true,
    stashInitialSize: 512000,

    isLive: true,

    liveBufferLatencyChasing: true,
    liveBufferLatencyChasingOnPaused: false,
    liveBufferLatencyMaxLatency: 3,
    liveBufferLatencyMinRemain: 1.0,

    liveSync: true,
    liveSyncMaxLatency: 3.5,
    liveSyncTargetLatency: 1.8,
    liveSyncPlaybackRate: 1.1,

    lazyLoad: true,
    lazyLoadMaxDuration: 3 * 60,
    lazyLoadRecoverDuration: 30,
    deferLoadAfterSourceOpen: true,

    autoCleanupSourceBuffer: true,
    autoCleanupMaxBackwardDuration: 3 * 60,
    autoCleanupMinBackwardDuration: 2 * 60,

    statisticsInfoReportInterval: 600,

    fixAudioTimestampGap: true,

    accurateSeek: false,
    seekType: 'range',  // [range, param, custom]
    seekParamStart: 'bstart',
    seekParamEnd: 'bend',
    rangeLoadZeroStart: false,
    customSeekHandler: undefined,
    reuseRedirectedURL: false,
    // referrerPolicy: leave as unspecified

    headers: undefined,
    customLoader: undefined
};

export function createDefaultConfig() {
    return Object.assign({}, defaultConfig);
}
