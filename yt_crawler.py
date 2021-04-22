#! /usr/bin/env python
import os
import json
import time
import requests
import pytchat
from functools import partial
from tqdm import tqdm
tqdm = partial(tqdm, position=0, leave=True)

YOUTUBE_API_KEY = "Your_API_Key"

channel_dict = {'channel_id':'playlist_id'}
#example:{'UC1WBRpr2G-NQvm_-jv-i6gg':'UU1WBRpr2G-NQvm_-jv-i6gg',
#         'UCLZBXiS9ZrIXgKBs_SMfGBQ':'UULZBXiS9ZrIXgKBs_SMfGBQ'}

def main():
    yt_crawler = YTCrawler(YOUTUBE_API_KEY)
    for channel_id, playlist_id in channel_dict.items():
        videoId_list = yt_crawler.get_video_id(playlist_id)
        for video_id in tqdm(videoId_list):
            next_page_token = ''
            next_page_token_ = ''
            while 1:
                info, video_id_ = yt_crawler.get_video_info(video_id)
                info_list = []
                info_list.append(info)
                comments, next_page_token = yt_crawler.get_comments(video_id, page_token=next_page_token)
                replies, next_page_token_ = yt_crawler.get_comment_replies(video_id, page_token_=next_page_token_)
                livechat = yt_crawler.get_live_chat(video_id)
                video_data = {}  
                video_data["info"] = info_list
                video_data["comment"] = comments
                video_data["reply"] = replies
                video_data["livechat"] = livechat
                if not next_page_token:
                    break

            #if not os.path.exists(f'{channel_id}'):
                #os.mkdir(f'{channel_id}')
            with open(f'{channel_id}/{channel_id}_{video_id}.json', 'w') as outfile:
                json.dump(video_data, outfile, ensure_ascii=False)


class YTCrawler():
    def __init__(self, api_key):
        self.base_url = "https://www.googleapis.com/youtube/v3/"
        self.api_key = api_key

    def get_html_to_json(self, path):
        api_url = f"{self.base_url}{path}&key={self.api_key}"
        r = requests.get(api_url)
        if r.status_code == requests.codes.ok:
            data = r.json()
        else:
            data = None
        return data
    
    def get_video_id(self, playlist_id, part='snippet%2CcontentDetails'):
        fields = 'items(contentDetails(videoId%2CvideoPublishedAt)%2Csnippet%2Ftitle%2Cstatus)'
        path = f'playlistItems?part={part}&playlistId={playlist_id}&fields={fields}&maxResults=50'
        data = self.get_html_to_json(path)
        if data == None:
            return [], ''       
        
        videoId_list = []
        for data_item in data['items']:
            data_item = data_item['contentDetails']
            try:
                video_id = data_item['videoId']
                print(video_id)
            except ValueError:
                video_id = None
            videoId_list.append(video_id)    
        return videoId_list
        
    def get_video_info(self, video_id, part='snippet,statistics'):
        path = f'videos?part={part}&id={video_id}'
        data = self.get_html_to_json(path)
        
        if not data:
            return {}
        video_id_ = []
        data_item = data['items'][0]

        try:
            time_ = data_item['snippet']['publishedAt']
        except ValueError:
            time_ = None
        try:
            video_id_ = data_item['id']
        except KeyError:
            video_id_ = None            
            
        try:
            likeCount_ = data_item['statistics']['likeCount'] 
        except KeyError:
            likeCount_ = None
        try:            
            dislikeCount_ = data_item['statistics']['dislikeCount']
        except KeyError:
            dislikeCount_ = None            

        info = {
            'channelId': data_item['snippet']['channelId'],            
            'channelTitle': data_item['snippet']['channelTitle'],
            'title': data_item['snippet']['title'],
            'publishedAt': time_,
            'description': data_item['snippet']['description'],
            'videoId': data_item['id'],
            'likeCount': likeCount_,
            'dislikeCount': dislikeCount_,
            'commentCount': data_item['statistics']['commentCount'],
            'viewCount': data_item['statistics']['viewCount']
        }
        return info, video_id_

    def get_comments(self, video_id, page_token='', part='snippet', max_results=10000):
        path = f'commentThreads?part={part}&videoId={video_id}&maxResults={max_results}&pageToken={page_token}'
        data = self.get_html_to_json(path)
        if data == None:
            return [], ''       
        else:
            next_page_token = data.get('nextPageToken', '')
        

        comments = []
        for data_item in data['items']:
            data_item = data_item['snippet']
            top_comment = data_item['topLevelComment']
            try:
                time_ = top_comment['snippet']['publishedAt']
            except ValueError:
                time_ = None
            except ValueError:
                time_ = None
            if 'authorChannelId' in top_comment['snippet']:
                ru_id = top_comment['snippet']['authorChannelId']['value']
            else:
                ru_id = ''

            ru_name = top_comment['snippet'].get('authorDisplayName', '')
            if not ru_name:
                ru_name = ''
            
            comments.append({
                'videoId': video_id,
                'commentId': ru_id,
                'parentId': top_comment['id'],
                'authorDisplayName': ru_name,
                'textOriginal': top_comment['snippet']['textOriginal'],
                'likeCount': int(top_comment['snippet']['likeCount']),
                'publishAt': time_
            })
        return comments, next_page_token
    

                
    def get_comment_replies(self, video_id, page_token_='',part='replies', max_results=10000):
        path = f'commentThreads?part={part}&videoId={video_id}&maxResults={max_results}&pageToken={page_token_}'
        data_ = self.get_html_to_json(path)
        if data_ == None:
            return [], ''       
        else:
            next_page_token_ = data_.get('nextPageToken', '')        
        
        replies = []

        for data_item_ in data_['items']:
            if 'replies' in data_item_:
                data_item_ = data_item_['replies']
                reply_comment = data_item_['comments'][0]

                try:
                    time_ = reply_comment['snippet']['publishedAt']
                except KeyError:
                    time_ = None

                if 'authorChannelId' in reply_comment['snippet']:
                    ru_id = reply_comment['snippet']['authorChannelId']['value']
                else:
                    ru_id = ''

                ru_name = reply_comment['snippet'].get('authorDisplayName', '')
                if not ru_name:
                    ru_name = ''

                replies.append({
                    'videoId': reply_comment['snippet']['videoId'],
                    'replyId': ru_id,
                    'parentId': reply_comment['snippet']['parentId'],
                    'authorDisplayName': ru_name,
                    'textOriginal': reply_comment['snippet']['textOriginal'],
                    'likeCount': int(reply_comment['snippet']['likeCount']),
                    'publishAt': time_
                })
        return replies, next_page_token_
    
    def get_live_chat(self, video_id):
        video_id_ = video_id
        livechat = []
        chat = pytchat.create(video_id)
        if chat.is_alive():
            data = chat.get().json()
            if 'author' in data:
                chat_data = json.loads(data)
                for each_chat in chat_data:
                    chatId = each_chat['author']['channelId']
                    authorDisplayName =  each_chat['author']['name']
                    textOriginal = each_chat['message']
                    publishAt = each_chat['datetime']
                    elapsedTime = each_chat['elapsedTime']

                    livechat.append({
                    'videoId': video_id_,   
                    'chatId' : chatId,
                    'authorDisplayName' : authorDisplayName,
                    'textOriginal' : textOriginal,
                    'publishAt' : publishAt,
                    'elapsedTime' : elapsedTime
                    }) 
        else:
            livechat = []
        return livechat


if __name__ == "__main__":
    main()
