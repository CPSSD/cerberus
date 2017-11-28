FROM rustlang/rust:nightly

EXPOSE 8081

ADD target/release/master /bin/master

# ENTRYPOINT []
CMD ["/bin/master", "--port=8081"]
