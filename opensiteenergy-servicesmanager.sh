#!/bin/bash

# Script to setup SSL certificate and restart services once build has completed

while true
    do
        sleep 1

        if [ -f "/usr/src/openwindenergy/DOMAIN" ]; then
            . /usr/src/openwindenergy/DOMAIN
            sed -i "s/.*ServerName.*/    ServerName $DOMAIN/" /etc/apache2/sites-available/001-default-build-post.conf
            sed -i "s/.*ServerName.*/    ServerName $DOMAIN/" /etc/apache2/sites-available/002-default-build-pre.conf
            sudo /usr/sbin/apache2ctl restart
            sudo rm /usr/src/openwindenergy/log-certbot.txt
            sudo certbot --apache --non-interactive --agree-tos --email info@${DOMAIN} --domains ${DOMAIN} | sudo tee /usr/src/openwindenergy/log-certbot.txt >/dev/null
            if grep -q 'Successfully deployed certificate' /usr/src/openwindenergy/log-certbot.txt; then
                sudo cp /usr/src/openwindenergy/DOMAIN /usr/src/openwindenergy/DOMAINACTIVE
                sudo /usr/bin/systemctl restart tileserver.service
                sudo /usr/sbin/apache2ctl restart
            fi
            sudo rm /usr/src/openwindenergy/DOMAIN
        fi

        if [ -f "/usr/src/openwindenergy/RESTARTSERVICES" ]; then
            echo "Restarting tileserver.service and apache2 with post-build conf"
            sudo /usr/bin/systemctl restart tileserver.service
            sudo a2ensite 001-default-build-post.conf
            sudo a2dissite 002-default-build-pre.conf
            sudo /usr/sbin/apache2ctl restart

            # Handle case where certbot was run on 002-default-build-pre.conf but build has now switched to 001-default-build-post.conf
            if [ -f "/usr/src/openwindenergy/DOMAINACTIVE" ]; then
                if ! grep -q 'server-name' /etc/apache2/sites-available/001-default-build-post.conf; then
                    if ! grep -q 'letsencrypt' /etc/apache2/sites-available/001-default-build-post.conf; then
                        . /usr/src/openwindenergy/DOMAINACTIVE
                        sudo rm /usr/src/openwindenergy/log-certbot.txt
                        sudo certbot --apache --non-interactive --agree-tos --email info@${DOMAIN} --domains ${DOMAIN} | sudo tee /usr/src/openwindenergy/log-certbot.txt >/dev/null
                        sudo /usr/sbin/apache2ctl restart
                    fi
                fi
            fi

            rm /usr/src/openwindenergy/RESTARTSERVICES
        fi

        if [ -f "/usr/src/openwindenergy/OPENWINDENERGY-START" ]; then
            echo "Starting openwindenergy.service"
            sudo /usr/bin/systemctl restart openwindenergy.service
            rm /usr/src/openwindenergy/OPENWINDENERGY-START
        fi

        if [ -f "/usr/src/openwindenergy/OPENWINDENERGY-STOP" ]; then
            echo "Stopping openwindenergy.service"
            sudo /usr/bin/systemctl stop openwindenergy.service
            rm /usr/src/openwindenergy/OPENWINDENERGY-STOP
        fi

    done
