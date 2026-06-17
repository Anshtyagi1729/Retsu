package queue

const (
	//this is for redisssssnamesake
	PendingKey  = "retsu:pending"
	RetryKey    = "retsu:retry"
	InflightKey = "retsu:inflight"
	DLQkey      = "retsu:dlq"
)
type Queue struct{
	
}