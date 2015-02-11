package zazu

func flushdb() {
	conn := redisPool.Get()
	if _, err := conn.Do("FLUSHDB"); err != nil {
		panic(err)
	}
}
