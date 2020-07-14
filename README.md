
# Accenture
programming assignment from accenture labs

First you need to setup a mongo db (ubuntu server instructions):


## MONGO INSTALLATION
    ```
    sudo apt install docker.io
    sudo docker pull mongo
    sudo docker run -it --name mongodb -p 27017:27017 -d mongo
    ```


Then you can install the services and workers:

## APPLICATION INSTALLATION:
    ```
    clone the repo into your application server
    cd into API_SERVER
    pip install -r requirments.txt
    set MONGO_URL env var to the mongo server IP
    add your host IP to ALLOWED_HOSTS in settings
    python manage runserver 0.0.0.0:port
    on the first run the app will init the mongo db and populate it with data