FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /app

COPY ./*.csproj /app/
RUN dotnet restore

COPY ./*.cs* /app/
RUN dotnet publish -c Release -o out

FROM mcr.microsoft.com/dotnet/runtime:6.0 AS runtime
WORKDIR /app
VOLUME [ "/app/sqlite" ]
USER 1000

COPY --from=build /app/out ./

ENTRYPOINT ["./osu-onlinedb-generator"]
