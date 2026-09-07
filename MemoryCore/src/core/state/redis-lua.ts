/**
 * Lua scripts for RedisStateBackend — atomic timer claim and capture.
 */

/** ZRANGEBYSCORE + ZREM atomically; returns claimed members. */
export const LUA_CLAIM_EXPIRED_TIMERS = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local batch = tonumber(ARGV[2])
local members = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'LIMIT', 0, batch)
for i = 1, #members do
  redis.call('ZREM', key, members[i])
end
return members
`;

/**
 * Atomic capture: optional buffer append, increment conversation_count,
 * enqueue task on threshold or set timer.
 *
 * KEYS[1]=stateHash, KEYS[2]=bufferList, KEYS[3]=timerZset, KEYS[4]=taskStream
 * ARGV[1]=messageJson (empty skip), ARGV[2]=rounds, ARGV[3]=threshold,
 * ARGV[4]=nowMs, ARGV[5]=fireAtMs, ARGV[6]=timerMember (instanceId\\x00rest),
 * ARGV[7]=taskJson, ARGV[8]=defaultStateJson
 */
export const LUA_CAPTURE_ATOMIC = `
local stateKey = KEYS[1]
local bufKey = KEYS[2]
local timerKey = KEYS[3]
local streamKey = KEYS[4]

local messageJson = ARGV[1]
local rounds = tonumber(ARGV[2])
local threshold = tonumber(ARGV[3])
local nowMs = tonumber(ARGV[4])
local fireAtMs = tonumber(ARGV[5])
local timerMember = ARGV[6]
local taskJson = ARGV[7]
local defaultState = ARGV[8]

if messageJson ~= '' then
  redis.call('RPUSH', bufKey, messageJson)
end

local countStr = redis.call('HGET', stateKey, 'conversation_count')
local count = 0
if countStr then count = tonumber(countStr) end
count = count + rounds
redis.call('HSET', stateKey, 'conversation_count', count, 'last_active_time', nowMs)

if count >= threshold then
  redis.call('XADD', streamKey, '*', 'payload', taskJson)
  redis.call('ZREM', timerKey, timerMember)
  redis.call('HSET', stateKey, 'conversation_count', 0)
  return {1, 0}
else
  redis.call('ZADD', timerKey, fireAtMs, timerMember)
  return {0, count}
end
`;

/** SET lock NX PX; returns 1 if acquired. */
export const LUA_ACQUIRE_LOCK = `
if redis.call('GET', KEYS[1]) == false then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
`;

/** Renew lock only if owner matches. */
export const LUA_RENEW_LOCK = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`;

/** Release lock only if owner matches. */
export const LUA_RELEASE_LOCK = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;
