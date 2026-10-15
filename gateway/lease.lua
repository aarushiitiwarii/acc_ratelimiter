-- Atomic lease: token bucket (smooth refill) + sliding-window counter (hard cap).
-- KEYS[1] = bucket key
-- ARGV[1] = capacity (burst)           ARGV[2] = refill tokens/sec
-- ARGV[3] = tokens requested (batch)   ARGV[4] = window ms (0 = disabled)
-- ARGV[5] = max requests per window
-- Returns the number of tokens granted (0..requested).
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts', 'wstart', 'wcur', 'wprev')
local cap = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local req = tonumber(ARGV[3])
local win = tonumber(ARGV[4]) or 0
local wlimit = tonumber(ARGV[5]) or 0

-- 1. token bucket: Tokens_new = min(Capacity, Tokens_old + dt * RefillRate)
local tokens = tonumber(d[1]) or cap
local ts = tonumber(d[2]) or now
tokens = math.min(cap, tokens + math.max(0, now - ts) / 1000 * rate)
local granted = math.floor(math.min(tokens, req))

-- 2. sliding window: weighted previous window + current window
local wstart, wcur, wprev = 0, 0, 0
if win > 0 and wlimit > 0 then
  local aligned = now - (now % win)
  wstart = tonumber(d[3]) or aligned
  wcur = tonumber(d[4]) or 0
  wprev = tonumber(d[5]) or 0
  if wstart < aligned then
    if aligned - wstart >= 2 * win then wprev = 0 else wprev = wcur end
    wcur = 0
    wstart = aligned
  end
  local weight = 1 - (now - wstart) / win
  local room = math.floor(wlimit - (wprev * weight + wcur))
  if room < 0 then room = 0 end
  if granted > room then granted = room end
  wcur = wcur + granted
end

redis.call('HSET', KEYS[1], 'tokens', tokens - granted, 'ts', now,
           'wstart', wstart, 'wcur', wcur, 'wprev', wprev)
local ttl = math.ceil(cap / math.max(rate, 0.001) * 2000)
if win > 0 and ttl < win * 3 then ttl = win * 3 end
redis.call('PEXPIRE', KEYS[1], ttl)
return granted
