local inFlightKey = KEYS[#KEYS]
local inFlightScore = ARGV[1]
for keyNum = 1, #KEYS - 1 do
    local item = redis.call('rpop', KEYS[keyNum])
    if item then
        redis.call('zadd', inFlightKey, inFlightScore, item)
        return item
    end
end
return nil
