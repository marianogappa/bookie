FROM alpine:3.4

ADD bookie /bookie

ENTRYPOINT ["/bookie"]
CMD ["-config=/mnt/bookie/config.json"]
