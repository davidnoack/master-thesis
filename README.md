# SHSDB Stream Processing Application
Designing a stream processing application for micro data

# Prerequisites:
- JDK (>=11), with JAVA_HOME correctly set.
- Apache Maven (>=3.6.2), with MAVEN_HOME correctly set.
- Installed Docker and Docker Compose.

# How to run/test?

1. Clone.
2. In the project root folder, run "docker-compose up".
3. Open "http://localhost:9527" in Browser.
4. Login with username "pulsar" and password "pulsar".
5. Add "New Environment" with random name and URL "http://standalone:8080".
6. Select newly created environment, navigate to "Namespaces" and add new namespace "longterm".
7. In each subproject folder, execute "mvn compile quarkus:dev".
8. Send POST request with report CSV in body to "http://localhost:8081".
9. Send POST request with CSDB CSV in body to "http://localhost:8082".
10. Open "http://localhost:8083" in browser.
