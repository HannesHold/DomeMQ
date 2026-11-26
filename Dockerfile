# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG VERSION=0.1.0
WORKDIR /src
COPY Directory.Build.props ./
COPY assets/icon/domemq.ico assets/icon/domemq.ico
COPY src/DomeMQ.Protocol/ src/DomeMQ.Protocol/
COPY src/DomeMQ.Server/ src/DomeMQ.Server/
COPY src/DomeMQ.Broker/ src/DomeMQ.Broker/
RUN dotnet publish src/DomeMQ.Broker/DomeMQ.Broker.csproj \
    -c Release \
    -o /out \
    -p:Version=${VERSION}

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
ARG VERSION=0.1.0
LABEL org.opencontainers.image.title="DomeMQ" \
      org.opencontainers.image.description="Cross-platform IPC message broker" \
      org.opencontainers.image.source="https://github.com/HannesHold/DomeMQ" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"
WORKDIR /app
COPY --from=build /out/ ./
VOLUME ["/config"]
ENV DOTNET_ENVIRONMENT=Production \
    DOTNET_CLI_TELEMETRY_OPTOUT=1
EXPOSE 5071
ENTRYPOINT ["dotnet", "DomeMQ.Broker.dll"]
