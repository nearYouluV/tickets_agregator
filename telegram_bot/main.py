from flask import Flask, request,  render_template, redirect, url_for, flash
import telebot
from telebot.apihelper import ApiException
from time import sleep
import os
from typing import TYPE_CHECKING
from threading import Thread
import json
from bot import msg_markup, bot
from shared.config import check_subscription
from shared.models import Users,  Session, AdminUser
import logging
from flask.logging import default_handler
from buttons import create_deal_msg
from shared.rabit_config import get_connection, RMQ_ROUTING_KEY
from flask_login import login_user, LoginManager
from admin import admin_bp  
import datetime
if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import BasicProperties, Basic


app = Flask(__name__)
app.register_blueprint(admin_bp)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY')
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
app.logger.removeHandler(default_handler)
WEBHOOK_URL_PATH = "/webhook"
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


@app.template_filter('date')
def format_date(value, format='%d/%m/%Y'):
    if isinstance(value, datetime.datetime):
        return value.strftime(format)
    return value

@login_manager.user_loader
def load_user(user_id):
    with Session() as session:
        return session.query(AdminUser).get(int(user_id))


@login_manager.user_loader
def load_user(user_id):    
    with Session() as session:
        return session.query(AdminUser).get(int(user_id))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        with Session() as session:
            user = session.query(AdminUser).filter_by(username=username).first()
            if user and user.check_password(password):
                login_user(user)
                flash('Успешный вход!', 'success')
                return redirect(url_for('admin.index'))  
            else:
                flash('Неверное имя пользователя или пароль', 'danger')
    
    return render_template('login.html')

    

@app.route(WEBHOOK_URL_PATH, methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.stream.read().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200


def set_webhook():
    bot.remove_webhook()
    public_url = os.getenv('WEBHOOK_URL')
    bot.set_webhook(url=public_url + WEBHOOK_URL_PATH)
    log.debug('Webhook is set')


def send_message(row: dict):
    with Session() as session:

        sent_msgs_dict = {}

        users = session.query(Users).all()
        for user in users:
            user_airports = user.Airports.split('\n')
            user_id = user.ID


            if not check_subscription(user_id):
                continue

            elif user_id not in sent_msgs_dict:
                sent_msgs_dict[user_id] = set() 
            
            elif row['ID'] in sent_msgs_dict[user_id]:
                continue

            elif not bool(set(user_airports) & set( row['DepartureAirports'].split(','))):
                continue

            msg = create_deal_msg(row)
            try:
                base_path = os.getcwd()
                photo_path = os.path.join(base_path, f'imgs/{row["PictureName"]}')
                
                if row['PictureName']:
                    try:
                        with open(photo_path, 'rb') as photo:
                            bot.send_photo(user_id, photo=photo)
                    except Exception as e:
                        print(f"Ошибка отправки фото: {e}")

                try:
                    bot.send_message(user_id, msg, parse_mode='HTML', reply_markup=msg_markup(row['ID']))
                    sleep(1)
                except Exception as e:
                    print(f"Ошибка отправки сообщения: {e}")

            except ApiException as e:
                if e.error_code == 403 and "bot was blocked by the user" in e.result_json["description"]:
                    print(f"Пользователь {user_id} заблокировал бота.")
                    user.ActiveUser = False
                    session.commit()
                    continue
                else:
                    pass
            sent_msgs_dict[user_id].add(row['ID'])


def handle_channel_message(message):
    with Session() as session:
        users = session.query(Users).all()
        session.commit()
        for user in users:
            try:
                if message.content_type == 'text':
                    bot.send_message(user.ID, message.text)
                elif message.content_type == 'photo':
                    photo_id = message.photo[-1].file_id
                    bot.send_photo(user.ID, photo_id, caption=message.caption)
                elif message.content_type == 'video':
                    video_id = message.video.file_id
                    bot.send_video(user.ID, video_id, caption=message.caption)
                elif message.content_type == 'document':
                    document_id = message.document.file_id
                    bot.send_document(user.ID, document_id, caption=message.caption)
                elif message.content_type == 'audio':
                    audio_id = message.audio.file_id
                    bot.send_audio(user.ID, audio_id, caption=message.caption)
                elif message.content_type == 'voice':
                    voice_id = message.voice.file_id
                    bot.send_voice(user.ID, voice_id, caption=message.caption)
                elif message.content_type == 'sticker':
                    sticker_id = message.sticker.file_id
                    bot.send_sticker(user.ID, sticker_id)
                elif message.content_type == 'animation':
                    animation_id = message.animation.file_id
                    bot.send_animation(user.ID, animation_id, caption=message.caption)
                elif message.content_type == 'video_note':
                    video_note_id = message.video_note.file_id
                    bot.send_video_note(user.ID, video_note_id)
                else:
                    pass
            except ApiException as e:
                if e.error_code == 403 and "bot was blocked by the user" in e.result_json["description"]:
                    user_db = session.query(Users).filter(Users.ID == user.ID).first()
                    user_db.ActiveUser = False
                    session.commit()
                else:
                    print(f"Error sending message to user {user.ID}: {e}")
                    sleep(50)


def process_new_message(ch:"BlockingChannel",
                         method:"Basic.Deliver",
                           properties:"BasicProperties",
                             body:bytes, ):
    
    
    msg = json.loads(body.decode('utf-8'))
    if not msg['Type'] == 'technical message':
        send_message(msg)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_message(ch:"BlockingChannel", routing_key:str):
    ch.basic_consume(
        queue=routing_key,
        on_message_callback=process_new_message,
    )
    ch.start_consuming()

def run_consumer():
    with get_connection() as connection:
        with connection.channel() as channel:
            consume_message(channel, RMQ_ROUTING_KEY)

    
@bot.channel_post_handler(content_types=['text', 'photo', 'audio', 'voice', 'sticker', 'animation', 'video_note', 'document'])
def monitor_channel_posts(message):
    if str(message.chat.id) == os.getenv('CHANNEL_UPDATES_ID'):
        return
    handle_channel_message(message)

def main():
    set_webhook() 
    sleep(45)
    Thread(target=run_consumer).start()
    app.run(host="0.0.0.0", port=8081)


if __name__ == '__main__':
    main()