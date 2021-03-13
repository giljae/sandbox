var Resilient = require('resilient')

var client = Resilient({
  service: {
      servers: [
        'http://127.0.0.1:8081',
        'http://127.0.0.1:8082',
        'http://127.0.0.1:8083',
      ],
      timeout: 1000,
      retry: 1,
      waitBeforeRetry: 300
  },
  balancer: {
    //roundRobin: true
    random: true
  }
})

// Target Server가 미리 구동되어 있어야 함
client.get('/normal',function(err,res) {
  if(res.status === 200) {
    console.log('Success:',res.data)
  } else {
    console.log('Fail:', err.data)
  }
})
