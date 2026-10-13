-- KEYS[1] = bucket key
-- ARGV[1] = capacity, ARGV[2] = refill tokens per second, ARGV[3] = tokens requested
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local cap = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local req = tonumber(ARGV[3])
local tokens = tonumber(d[1]) or cap
local ts = tonumber(d[2]) or now
tokens = math.min(cap, tokens + math.max(0, now - ts) / 1000 * rate)
local granted = math.floor(math.min(tokens, req))
redis.call('HSET', KEYS[1], 'tokens', tokens - granted, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(cap / rate * 2000))
return granted