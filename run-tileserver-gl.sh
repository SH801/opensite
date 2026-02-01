#! /bin/bash -l

export LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH
export PKG_CONFIG_PATH=/opt/lib/pkgconfig:$PKG_CONFIG_PATH
export EXTERNAL_IP=$(curl ipinfo.io/ip)
export PUBLIC_URL=http://${EXTERNAL_IP}/tiles/

if [ -f "/usr/src/openwindenergy/DOMAINACTIVE" ]; then
    . /usr/src/openwindenergy/DOMAINACTIVE
    export PUBLIC_URL=https://${DOMAIN}/tiles/
fi


xvfb-run --server-args="-screen 0 1024x768x24" tileserver-gl -p 8080 --public_url ${PUBLIC_URL} --config build-cli/tileserver/config.json