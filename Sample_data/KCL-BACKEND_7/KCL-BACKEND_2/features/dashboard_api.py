from flask import Flask, request, jsonify, send_file, Blueprint
from flask_cors import CORS
from pymongo import MongoClient
from datetime import datetime, timedelta
import re
from email_validator import validate_email, EmailNotValidError
import pandas as pd
import os
from flask_swagger_ui import get_swaggerui_blueprint
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from dotenv import load_dotenv
import googlemaps
import pymongo.errors
from controller.dashboard_api_controller import repeat_dict, resolve_lang, pick_dict
from ETL.ETL_utils_ import db, conn, list_s3_images
# ------------------------------
# Load environment variables
# ------------------------------
load_dotenv()

# ------------------------------
# Flask App Setup
# ------------------------------
app = Flask(__name__)
CORS(app)

# JWT configuration
app.config['JWT_SECRET_KEY'] = os.getenv("SECRET_KEY", "default_secret_key")
jwt_exp_minutes = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRES_MINUTES", 30))
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(minutes=jwt_exp_minutes)
jwt = JWTManager(app)

googlemaps_key = os.getenv("googlemaps_key", None)
gmaps = googlemaps.Client(key=googlemaps_key)

dashboard_bp = Blueprint("dashboard", __name__)
# ------------------------------
# Custom JWT error handlers
# ------------------------------
@jwt.unauthorized_loader
def custom_unauthorized_response(err_str):
    return jsonify({"statusCode": 401, "message": "Invalid user", "response": {}})

@jwt.invalid_token_loader
def custom_invalid_token_response(err_str):
    return jsonify({"statusCode": 422, "message": "Invalid token", "response": {}})

@jwt.expired_token_loader
def custom_expired_token_response(jwt_header, jwt_payload):
    return jsonify({"statusCode": 401, "message": "Token expired, please login again", "response": {}})




#--------------------------------------------------
#   Dashboard Images
#--------------------------------------------------


@dashboard_bp.route("/api/dashboard_images", methods=["POST"])
@jwt_required(optional=True)
def dashboard_images():
    try:
        language_code = resolve_lang(request.headers.get("languageCode", None))
        mobile = request.json.get('mobile', None)

        if language_code is None or language_code == "":
            return jsonify({
                "statusCode": 400,
                "message": "Language code is required in headers",
                "response": {}
            })

        dashboard_info_lst = db.dashboard_info.find({"isvisible":True},{'_id':0})
        dashboard_info_df = pd.DataFrame(dashboard_info_lst)
        dashboard_info_df = dashboard_info_df.rename(columns={'code':'index','name':'type'})
        del dashboard_info_lst
        dasbord_info = dashboard_info_df.to_dict('records')
        del dashboard_info_df


        banner_info_lst = db.bannerlist_master.find({}, {'_id': 0})
        banner_info_df = pd.DataFrame(banner_info_lst)
        banner_info_df = banner_info_df.rename(columns={'bannerlist_id': 'id', "image_url": "image"})
        del banner_info_lst
        banner_info = banner_info_df.to_dict('records')
        del banner_info_df


        contest_info_lst = db.contest_master_copy.find({}, {'_id': 0, "contest_id":1, "image_url":1, "contest_name": 1})

        contest_info_df = pd.DataFrame(contest_info_lst)
        contest_info_df["contest_name_"+language_code] = contest_info_df["contest_name"].apply(
            lambda x: x.get(language_code) if isinstance(x, dict) else ""
        )
        del contest_info_df["contest_name"]
        if 'contest_name_'+language_code not in contest_info_df.columns:
            contest_info_lst = db.contest_master_copy.find({}, {'_id': 0, "contest_id": 1, "image_url": 1,
                                                           "contest_name_en" : 1})

            contest_info_df = pd.DataFrame(contest_info_lst)
            contest_info_df["contest_name_en"] = contest_info_df["contest_name"].apply(
                lambda x: x.get("en") if isinstance(x, dict) else ""
            )
            del contest_info_df["contest_name"]
            contest_info_df = contest_info_df.rename(
                columns={'contest_name_en': 'description'})
        contest_info_df = contest_info_df.rename(columns={ 'contest_name_'+language_code: 'description'})



        contest_date_lst = db.contest_date.find({}, {'_id': 0, "contest_id": 1,
                                                       "date_" + language_code: 1})
        contest_date_df = pd.DataFrame(contest_date_lst)
        if 'contest_name_' + language_code not in contest_date_df.columns:
            contest_date_lst = db.contest_date.find({}, {'_id': 0, "contest_id": 1,
                                                           "date_" + language_code: 1})
            contest_date_df = pd.DataFrame(contest_date_lst)
            contest_date_df = contest_date_df.rename(columns={"date_en": "date" })
        contest_date_df = contest_date_df.rename(columns={"date_" + language_code: "date" })
        contest_date_df = contest_date_df.astype({'contest_id':str})
        contest_info_df = pd.merge(contest_info_df, contest_date_df, on=['contest_id'])

        contest_info_df = contest_info_df.rename(columns={"contest_id":"id", "image_url":"image"})
        del contest_info_lst
        contest_info = contest_info_df.to_dict('records')
        del contest_info_df,contest_date_df



        team_info_lst = db.team_master_copy.find({}, {'_id': 0, "team_id": 1, "image_url": 1,
                                                       "team_name": 1})

        team_info_df = pd.DataFrame(team_info_lst)
        team_info_df["team_name_" + language_code] = team_info_df["team_name"].apply(
            lambda x: x.get(language_code) if isinstance(x, dict) else ""
        )

        del team_info_df["team_name"]
        if 'team_name_' + language_code not in team_info_df.columns:
            team_info_lst = db.team_master_copy.find({}, {'_id': 0, "team_id": 1, "image_url": 1,
                                                           "team_name_en": 1})

            team_info_df = pd.DataFrame(team_info_lst)
            team_info_df["team_name_en"] = team_info_df["team_name"].apply(
            lambda x: x.get(language_code) if isinstance(x, dict) else ""
            )
            team_info_df = team_info_df.rename(columns={ "team_name_en" : "description"})
        team_info_df = team_info_df.rename(columns={'team_id': 'id',"team_name_"+ language_code : "description", "image_url":"image"})
        del team_info_lst
        team_info = team_info_df.to_dict('records')
        del team_info_df


        contestant_info_lst = db.contestant_master_copy.find({}, {'_id': 0, "contestant_id": 1, "image_url": 1,
                                                 "contestant_name": 1, "video_url":1, "top_contestant_name":1})
        contestant_info_df = pd.DataFrame(contestant_info_lst)
        contestant_info_df["contestant_name_" + language_code] = contestant_info_df["contestant_name"].apply(
            lambda x: x.get(language_code) if isinstance(x, dict) else ""
        )

        contestant_info_df["top_contestant_name_" + language_code] = contestant_info_df["top_contestant_name"].apply(
            lambda x: x.get(language_code) if isinstance(x, dict) else ""
        )

        del contestant_info_df["contestant_name"]
        del contestant_info_df["top_contestant_name"]

        if 'contestant_name_' + language_code not in contestant_info_df.columns:
            contestant_info_lst = db.contestant_master_copy.find({}, {'_id': 0, "contestant_id": 1, "image_url": 1,
                                                     "contestant_name_en": 1, "video_url":1, "top_contestant_name":1})
            contestant_info_df = pd.DataFrame(contestant_info_lst)
            contestant_info_df["contestant_name_en"] = contestant_info_df["contestant_name"].apply(
                lambda x: x.get(language_code) if isinstance(x, dict) else ""
            )
            contestant_info_df["top_contestant_name_en"] = contestant_info_df[
                "top_contestant_name"].apply(
                lambda x: x.get(language_code) if isinstance(x, dict) else ""
            )
            del contestant_info_df["contestant_name"]
            del contestant_info_df["top_contestant_name"]
            contestant_info_df = contestant_info_df.rename(
                columns={
                         "contestant_name_en": "description", "top_contestant_name_en":"name"})
        contestant_info_df = contestant_info_df.rename(columns={'contestant_id': 'id', "image_url":"video_thumbnail","video_url":"video", "contestant_name_" + language_code :"description","top_contestant_name_"+language_code:"name" })
        del contestant_info_lst

        contestant_info = contestant_info_df.to_dict('records')
        del contestant_info_df


        episodes_info_lst = db.episodes_master_copy.find({}, {'_id': 0, "episode_id": 1, "thumbnail": 1,
                                                             "episode_title": 1, "video_url": 1})
        episodes_info_df = pd.DataFrame(episodes_info_lst)
        episodes_info_df["episode_title_" + language_code] = episodes_info_df["episode_title"].apply(
            lambda x: x.get(language_code) if isinstance(x, dict) else ""
        )
        del episodes_info_df["episode_title"]
        if 'episode_title_' + language_code not in episodes_info_df.columns:
            episodes_info_lst = db.episodes_master_copy.find({}, {'_id': 0, "episode_id": 1, "thumbnail": 1,
                                                                 "episode_title_en": 1, "video_url": 1})
            episodes_info_df = pd.DataFrame(episodes_info_lst)
            episodes_info_df["episode_title_en"] = episodes_info_df["episode_title"].apply(
                lambda x: x.get(language_code) if isinstance(x, dict) else ""
            )
            del episodes_info_df["episode_title"]
            episodes_info_df = episodes_info_df.rename(
                columns={
                    "episode_title_en": "name"})
        episodes_info_df = episodes_info_df.rename(
            columns={'episode_id': 'id', "thumbnail": "video_thumbnail", "video_url": "video",
                     "episode_title_" + language_code: "description"})
        del episodes_info_lst
        episodes_info = episodes_info_df.to_dict('records')
        del episodes_info_df



        training_info_lst = db.training_videos_master_copy.find({}, {'_id': 0, "video_id": 1, "thumbnail": 1,
                                                         "video_title": 1, "video_url": 1})
        training_info_df = pd.DataFrame(training_info_lst)
        training_info_df["video_title_" + language_code] = training_info_df["video_title"].apply(
            lambda x: x.get(language_code) if isinstance(x, dict) else ""
        )
        del training_info_df["video_title"]
        if 'video_title_' + language_code not in training_info_df.columns:
            training_info_lst = db.episodes_master_copy.find({}, {'_id': 0, "video_id": 1, "thumbnail": 1,
                                                             "video_title_en": 1, "video_url": 1})
            training_info_df = pd.DataFrame(training_info_lst)
            training_info_df["video_title_en"] = training_info_df["video_title"].apply(
                lambda x: x.get(language_code) if isinstance(x, dict) else ""
            )
            del training_info_df["video_title"]
            training_info_df = training_info_df.rename(
                columns={
                    "video_title_en": "name"})
        training_info_df = training_info_df.rename(
            columns={'video_id': 'id', "thumbnail": "video_thumbnail", "video_url": "video",
                     "video_title_" + language_code: "description"})
        del training_info_lst
        training_info = training_info_df.to_dict('records')
        del training_info_df

        contest_info=repeat_dict(contest_info)
        team_info=repeat_dict(team_info)
        contestant_info=repeat_dict(contestant_info)
        training_info=repeat_dict(training_info)
        episodes_info=repeat_dict(episodes_info)

        #print(dasbord_info)
        dic_list = [banner_info,contest_info,team_info,contestant_info,episodes_info,training_info]
        # Option 2: classic range indexing
        for i in range(len(dasbord_info)):
            dasbord_info[i][dasbord_info[i]["type"]] = dic_list[i]


        # --- build sections exactly as before (bannersList, contestThisSectionsList, etc.) ---


        return jsonify({
            "message": "Success",
            "response": dasbord_info,
            "statusCode": 200
        })


    except Exception as e:
        print(f"Error fetching dashboard data: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})



@dashboard_bp.route("/api/dashboard_more_option", methods=["POST"])
@jwt_required(optional=True)
def dashboard_more_option():
    try:
        mobile = request.json.get('mobile', None)
        language_code = resolve_lang(request.headers.get("languageCode", None))

        if language_code is None or language_code == "":
            return jsonify({
                "statusCode": 400,
                "message": "Language code is required in headers",
                "response": {}
            })

        contest_info_lst = db.contest_master_copy.find({},
                                                       {'_id': 0, "contest_id": 1, "image_url": 1, "contest_name": 1})

        contest_info_df = pd.DataFrame(contest_info_lst)
        contest_info_df["contest_name_" + language_code] = contest_info_df["contest_name"].apply(
            lambda x: x.get(language_code) if isinstance(x, dict) else ""
        )
        del contest_info_df["contest_name"]
        if 'contest_name_' + language_code not in contest_info_df.columns:
            contest_info_lst = db.contest_master_copy.find({}, {'_id': 0, "contest_id": 1, "image_url": 1,
                                                                "contest_name_en": 1})

            contest_info_df = pd.DataFrame(contest_info_lst)
            contest_info_df["contest_name_en"] = contest_info_df["contest_name"].apply(
                lambda x: x.get("en") if isinstance(x, dict) else ""
            )
            del contest_info_df["contest_name"]
            contest_info_df = contest_info_df.rename(
                columns={'contest_name_en': 'description'})
        contest_info_df = contest_info_df.rename(columns={'contest_name_' + language_code: 'description'})

        contest_date_lst = db.contest_date.find({}, {'_id': 0, "contest_id": 1,
                                                     "date_" + language_code: 1})
        contest_date_df = pd.DataFrame(contest_date_lst)
        if 'contest_name_' + language_code not in contest_date_df.columns:
            contest_date_lst = db.contest_date.find({}, {'_id': 0, "contest_id": 1,
                                                         "date_" + language_code: 1})
            contest_date_df = pd.DataFrame(contest_date_lst)
            contest_date_df = contest_date_df.rename(columns={"date_en": "date"})
        contest_date_df = contest_date_df.rename(columns={"date_" + language_code: "date"})
        contest_date_df = contest_date_df.astype({'contest_id': str})
        contest_info_df = pd.merge(contest_info_df, contest_date_df, on=['contest_id'])

        contest_info_df = contest_info_df.rename(columns={"contest_id": "id", "image_url": "image"})
        del contest_info_lst
        contest_info = contest_info_df.to_dict('records')
        del contest_info_df, contest_date_df
        more_info_docs = list(db.dashboard_more_info.find({}, {"_id": 0}))

        sections = []
        for doc in more_info_docs:
            title = pick_dict(doc.get("name"), language_code)

            if doc.get("code") == 1:
                data_slice = contest_info[0:4]
            elif doc.get("code") == 2:
                data_slice = contest_info[4:8]
            else:
                data_slice = []

            sections.append({
                "title": title,
                "visible": doc.get("isvisible", True),
                "data": data_slice
            })

        return jsonify({
            "message": "Success",
            "response": sections,
            "statusCode": 200
        })


    except Exception as e:
        print(f"Error fetching dashboard data: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})

