FROM --platform=$BUILDPLATFORM alpine/curl:latest AS plugin-provider

WORKDIR /app

RUN apk add --no-cache bash

COPY --chmod=755 ./get-plugin.sh get-plugin.sh

RUN ./get-plugin.sh

FROM trinodb/trino:477 AS base

COPY --chmod=755 ./entrypoint.sh /usr/bin/docker-entrypoint.sh

COPY --from=plugin-provider /usr/lib/trino/plugin/hudi/ /usr/lib/trino/plugin/hudi/

# to avoid permission issues when running entrypoint script
USER root

ENTRYPOINT [ "/usr/bin/docker-entrypoint.sh" ]

CMD ["/usr/lib/trino/bin/run-trino"]
