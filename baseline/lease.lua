-- KEYS[1] = shared bucket key
-- ARGV[1] = capacity
-- ARGV[2] = refill tokens per second
-- ARGV[3] = requested tokens

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local cap = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local req = tonumber(ARGV[3])

if not cap or not rate or not req or cap <= 0 or rate < 0 or req <= 0 then
    return redis.error_reply('invalid rate limit arguments')
end

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if not tokens then tokens = cap end
if not ts then ts = now end

local elapsed = math.max(0, now - ts) / 1000
tokens = math.min(cap, tokens + elapsed * rate)

local granted = math.min(req, math.floor(tokens))
tokens = tokens - granted

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)

local ttl
if rate > 0 then
    ttl = math.ceil(cap / rate * 2000)
else
    ttl = 86400000
end
redis.call('PEXPIRE', KEYS[1], ttl)

return granted