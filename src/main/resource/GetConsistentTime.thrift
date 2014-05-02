namespace java consistent.generated

service ConsistentTimeService{
  i64 getTimestamp(),
  i64 getTimestamps(1:i32 range)
}