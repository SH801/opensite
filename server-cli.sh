#!/bin/bash

if [ -f ".env" ]; then
    . ./.env
fi

echo "Running tileserver-gl..."

docker kill opensiteenergy-tileserver

if [ -n "${BUILD_FOLDER+1}" ]; then
    docker run --name opensiteenergy-tileserver -d --rm -v "$BUILD_FOLDER"tileserver/:/data -p 8080:8080 maptiler/tileserver-gl --config config.json
else
    docker run --name opensiteenergy-tileserver -d --rm -v $(pwd)/build/tileserver/:/data -p 8080:8080 maptiler/tileserver-gl --config config.json
fi

. venv/bin/activate
uvicorn opensiteenergy:app --host 0.0.0.0 --port 8000 --log-level info

echo "Closing tileserver-gl..."

docker kill opensiteenergy-tileserver
