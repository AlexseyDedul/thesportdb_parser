How to use:

git clone https://git.prj365.com/dedul.an/thesportdb.git

cd thesportdb

python3 -m venv venv

. venv/bin/activate

pipenv install Pipfile

Create ENV-file (check example in .env_example file):

echo HOST="localhost" USER="user_name" PASS="user_password" DB="database_name" > .env

Run application:

pipenv run python3 run.py
