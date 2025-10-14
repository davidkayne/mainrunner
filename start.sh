set -e
python3 update.py && python3 -m bot &
gunicorn --bind 0.0.0.0:$PORT --workers 4 web.wserver:app