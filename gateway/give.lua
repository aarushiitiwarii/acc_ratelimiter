-- Drift compensation: return unused leased tokens to the shared bucket.
-- KEYS[1] = bucket key
-- ARGV[1] = capacity, ARGV[2] = tokens to return, ARGV[3] = window ms (0 = disabled)
-- Tokens never exceed capacity; the window counter is only credited if the
-- window the tokens were drawn from is still the current one.
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local cap = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
local win = tonumber(ARGV[3]) or 0
local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens')) or 0
redis.call('HSET', KEYS[1], 'tokens', math.min(cap, tokens + n))
if win > 0 then
  local t = redis.call('TIME')
  local now = t[1] * 1000 + math.floor(t[2] / 1000)
  local wstart = tonumber(redis.call('HGET', KEYS[1], 'wstart')) or 0
  if wstart == now - (now % win) then
    local wcur = tonumber(redis.call('HGET', KEYS[1], 'wcur')) or 0
    redis.call('HSET', KEYS[1], 'wcur', math.max(0, wcur - n))
  end
end
return 1
