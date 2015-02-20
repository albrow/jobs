# Jobs

A persistent and flexible background jobs library for go.

[![GoDoc](https://godoc.org/github.com/albrow/Jobs?status.svg)](https://godoc.org/github.com/albrow/Jobs)

Supports the following features:

 - A job can encapsulate any arbitrary functionality. A job can do anything
   which can be done in a go function.
 - A job can be one-off (only executed once) or recurring (scheduled to
   execute at a specific interval).
 - A job can be retried a specified number of times if it fails.
 - A job is persistent, with protections against power loss and other worst
   case scenarios.
 - Jobs can be executed by any number of concurrent workers accross any
   number of machines, and each job will only be executed once.
 - You can query the database to find out e.g. the number of jobs that are
   currently executing or how long a particular job took to execute.
 - Any job that permanently fails will have its error captured and stored.

### Why is it Useful

Jobs is intended to be used in web applications written in go. It is useful for
cases where you need to execute some long-running code, but you don't want your users
to wait for the code to execute before rendering a response. A good example is sending
a welcome email to your users after they sign up. You can use Jobs to schedule the email
to be sent asynchronously, and render a response to your user without waiting for the email
to be sent. You could use a goroutine to accomplish the same thing, but in the event of
a server restart or power loss, the email might never be sent. Jobs guarantees that the
email will be sent at some time, and allows you to spread the work between different
machines.

### Registering Job Types

Jobs in Jobs must be organized into discrete types. Here's an example of
how to register a job which sends a welcome email to users:

``` go
// We'll specify that we want the job to be retried 3 times before finally failing
welcomeEmailJobs, err := Jobs.RegisterJobType("welcomeEmail", 3, func(user *User) {
	msg := fmt.Sprintf("Hello, %s! Thanks for signing up for foo.com.", user.Name)
	if err := emails.Send(user.EmailAddress, msg); err != nil {
		// Panics will be captured by a Jobs worker, triggering a retry
		panic(err)
	}
})
```

### Scheduling a Job

After registering a job type, you can schedule a job using the Schedule or ScheduleRecurring
methods like so:

``` go
// The priority argument lets you choose how important the job is. Higher
// priority jobs will be executed first.
job, err := welcomeEmailJobs.Schedule(100, time.Now(), &User{EmailAddress: "foo@example.com"})
if err != nil {
	// Handle err
}
```

You can use the Job object returned by Schedule or ScheduleRecurring to check on the status of
the job or cancel it manually.

### Starting and Configuring Worker Pools

You can schedule any number of worker pools accross any number of machines, provided every machine
agrees on the definition of some number of job types. For each pool, you can specify the number of
workers and some other parameters. You are only allowed one pool per machine, and Jobs gives you a
single exported Pool object to enforce this.

Configure a pool, use Jobs.Config.Pool like so:

``` go
Jobs.Config.Pool.NumWorkers = 8
Jobs.Config.Pool.MinWait = 10 * time.Millisecond
```

You can start the pool by calling the Start method.

``` go
if err := Jobs.Pool.Start(); err != nil {
	// Handle err
}
```

Once started the Pool will continue to query the database for new jobs and delegate those jobs to
some number of concurrent workers. To stop the Pool manually, you can use Pool.Close(). Then you can
use Pool.Wait() to wait for all workers to finish their current jobs (if any).

Any go program that calls Pool.Start() should also wait for the workers to finish before exiting. You
can do so by wrapping Close and Wait in a defer statement like so:

``` go
func main() {
	defer func() {
		Jobs.Pool.Close()
		if err := Jobs.Pool.Wait(); err != nil {
			// Handle err
		}
	}
}
```

### License

Jobs is licensed under the MIT License. See the LICENSE file for more information.
