# Client Side Load Balancing Sample Project
## ribbon-client
Spring Boot기반의 Ribbon + Spring Retry + Spring Actuator 사용
1. RR 방식의 Load Balancing 기능 (Client-side)
2. RESTTemplate API call failure에 대한 Retry 기능 (Retry후에 Next target으로 전환)
3. Spring Actuator기반의 Ping 기능

## ribbon-server
테스트를 위한 서버
