import requests
import pandas as pd
from pathlib import Path
import boto3
from io import StringIO
import json
# need to add boto3 to requirements later

class News:

    def __init__(self, api_key:str, which_news:str = 'news', language:str = 'en', timeframe:int = 24, size:int = 10, 
        country:str='us', domainurl:str=['ibtimes.com','latimes.com','investorplace.com','popsci.com','thehill.com'],
        prioritydomain:str=None, q:str=None, qInTitle:str=None, qInMeta:str=None, category:str=None, domain:str=None,
         excludedomain:str=None, timezone:str=None, full_content:bool=None, image:bool=None, video:bool=None):

        self.base_url = 'https://newsdata.io/api/1/'
        self.api_key = api_key
        self.which_news = which_news
        self.language = language
        self.timeframe = timeframe
        self.prioritydomain = prioritydomain
        self.size = size
        self.q = q
        self.qInTitle = qInTitle
        self.qInMeta = qInMeta
        self.country = country
        self.category = category
        self.domain = domain
        self.domainurl = domainurl
        self.excludedomain = excludedomain
        self.timezone = timezone
        self.full_content = full_content
        self.image = image
        self.video = video

        if api_key is None:
            raise Exception('Please enter a valid API key. A valid key cannot be None.')
        
    def _build_params(self, param:str, value) -> dict:

        str_params = {'q','qInTitle','qInMeta','country','category','language','domain','domainurl','excludedomain',
                      'prioritydomain','timezone','page'}
        bool_params = {'full_content','image','video'}
        int_params = {'timeframe','size'}

        if param in str_params:
            if isinstance(value,list):
                value = ','.join(value)
            if not isinstance(value,str):
                raise TypeError(f'{param} should be of type string.')
        elif param in bool_params:
            if not isinstance(value,bool):
                raise TypeError(f'{param} should be of type bool.')
            value = int(value)
        elif param in int_params:
            if not isinstance(value,int):
                raise TypeError(f'{param} should be of type int.')

        return {param:value}
    
    def get_news(self, page:str = None) -> dict:

        """
        Get news data from NewsData.io API. Please visit https://newsdata.io/documentation/#about-newdata-api for more information on the
        available parameters.

        Args:
            which_news: choose one of three string values from ['news','crypto','archive'] to set the desired api endpoint.
                news = latest news
                crypto = crypto news
                archive = archived news

        Returns:
            A dictionary of articles and set parameters for a given api request

        Raises:
            Exception if response code is not 200

        """
        
        if self.which_news not in {'news','crypto','archive'}:
            raise Exception("Please choose a valid API endpoint. Options: 'latest','crypto','archive'")

        base_url = self.base_url + self.which_news

        url_params = {
            'q':self.q, 'qInTitle':self.qInTitle, 'qInMeta':self.qInMeta, 'timeframe':self.timeframe, 'country':self.country, 
            'category':self.category, 'language':self.language, 'domain':self.domain, 'domainurl':self.domainurl, 
            'excludedomain':self.excludedomain, 'prioritydomain':self.prioritydomain, 'timezone':self.timezone, 
            'full_content':self.full_content, 'image':self.image, 'video':self.video, 'size':self.size, 'page':page
        }

        params = {}
        for key, value in url_params.items():
            if value is not None:
                params.update(self._build_params(param = key, value = value))

        headers = {'X-ACCESS-KEY': self.api_key}
        
        response = requests.get(url = base_url, params = params, headers = headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Request to {base_url} failed. Status code: {response.status_code} Response: {response.text}")
        
    def next_page_news(self, response) -> dict:

        """
        NewsData.io API responses are received in chunks/pages. We need to input the "page" parameter of a GET request
        to receive more chunks of the full response. This is useful to get more information, but also to reduce duplication
        from further requests.

        Args:
            response: json response of a GET request

        Returns:
            The "next page" of a given API response
        """
        return self.get_news(page = str(response['nextPage']))


def json_news_to_df(
        data: json,
    ) -> pd.DataFrame:
    """ Converts api request object from JSON to a dataframe"""
    results = data['results']
    df = pd.json_normalize(results)
    return df

def rename_and_select_columns_news( 
        df:pd.DataFrame
    )->pd.DataFrame:
    """Performs transformation on dataframe produced from extract() function. Returns dataframe to be loaded """
    # renaming some columns from json response
    df_news_renamed = df.rename(columns={
        "status": "status",
        "totalResults": "totalResults",
        "title": "title",
        "link": "article_link",
        "source_id": "publisher",
        "source_priority": "publisher_priority",
        "keywords": "keywords",
        "creator": "author", 
        "pubDate": "publish_date",
        "image_url": "image_url",
        "video_url": "video_url",
        "description": "description", 
        "content": "article_contents", 
        "category": "category",     
        "country": "country",
        "language": "language" 
    })
    df_news_selected = df_news_renamed[['title', 'article_link', 'keywords', 'author', 'keywords', 'publish_date', 'article_contents', 'category', 'country', 'language']]
    # keep only 'title', 'article_link', 'keywords', 'author', 'keywords', 'publish_date', 'article', 'category', 'country', 'language'
    return df_news_selected

def download_from_s3(
        ACCESS_KEY: str,
        SECRET_KEY: str,
        s3_bucket: str,
        key: str
    ) -> pd.DataFrame:
    """Downloads a csv from S3 bucket"""
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name='us-east-1'
    )
    s3_object = s3.get_object(Bucket=s3_bucket, Key=key)
    s3_data = s3_object['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(s3_data))
    return df



def calculate_word_frequency(article_contents, word):
    return article_contents.lower().count(str(word).lower())

def determine_relative_grade_level(avg_grade_level):
    if avg_grade_level <= 5:
        return 'Elementary'
    elif avg_grade_level <= 8:
        return 'Middle School'
    else:
        return 'High School'

def process_articles(word_by_grade_level_data, newspaper_articles_data):
    word_by_grade_level_df = pd.DataFrame(word_by_grade_level_data)
    newspaper_articles_df = pd.DataFrame(newspaper_articles_data)

    results = []

    for index, row in newspaper_articles_df.iterrows():
        article = row['article_contents']
        title = row['title']

        for _, word_row in word_by_grade_level_df.iterrows():
            word = word_row['Word']
            grade_level = word_row['Grade_Lv']

            frequency = calculate_word_frequency(article, word)
            relative_grade_level = grade_level

            results.append({
                'title': title,
                'word': word,
                'frequency': frequency,
                'grade_level': grade_level
            })

    results_df = pd.DataFrame(results)
    return results_df
# Read data from CSV files
word_by_grade_level_data = pd.read_csv('/Users/boxbag/TheSweats-project1-DataEngCamp/etl_project/data/vocabulary_by_gradelv.csv')
raw_newspaper_data = {'status': 'success',
 'totalResults': 185,
 'results': [{'article_id': '0de7b03067bf6d2bf64b024ed5aa9c83',
   'title': 'Pentagon releases new website on UFOs',
   'link': 'https://thehill.com/policy/defense/4181503-pentagon-releases-new-website-ufos/',
   'keywords': ['Defense', 'David Grusch', 'UAPs', 'UFOs', 'website'],
   'creator': ['Brad Dress'],
   'video_url': None,
   'description': 'The Defense Department on Thursday released a new website that will provide official declassified information on UFOs, including pictures and videos, for the public to easily parse through. The website is the official page for the public to interact with All-domain Anomaly Resolution Office (AARO), a relatively new Pentagon office tasked with reviewing and analyzing...',
   'content': 'The Defense Department on Thursday released a new website that will provide official declassified information on UFOs, including pictures and videos, for the public to easily parse through. The website is the official page for the public to interact with the All-domain Anomaly Resolution Office (AARO), a relatively new Pentagon office tasked with reviewing and analyzing UFOs. The site appears to still be under construction, but it . The Hill has reached out to the Defense Department for more information about when the full website will go live. The U.S. government, which now refers to UFOs by the name of Unidentified Aerial Phenomena (UAPs), has taken the presence of unknown flying objects more seriously in the past few years, as has Congress. In a Thursday release about the website, the Pentagon said it was “committed to transparency with the American people on AARO’s work” on UAPs. “This website will serve as a one-stop shop for all publicly available information related to AARO and UAP,” the release reads, “and AARO will regularly update the website with its most recent activities and findings as new information is cleared for public release.” The AARO website will allow the public to review photos and videos of UAPs as they are declassified and will publish reports, press releases and a “frequently asked questions” section about the phenomena. Users can also find available aircraft, balloon and satellite tracking sites on the page. In the fall, AARO intends to create a contact form for former U.S. government employees or others with knowledge of federal government programs to easily submit a report if they have relevant information related to UAPs. Since its inception in 2022, AARO has investigated about 800 UAPs. Some of the phenomena have innocuous explanations, but many others remain mysterious and unexplained. UAP interest grew this year after former intelligence official David Grusch claimed the evidence related to extraterrestrial craft and lifeforms. Grusch was unable to provide evidence at a House hearing this summer.',
   'pubDate': '2023-08-31 21:46:32',
   'image_url': 'https://thehill.com/wp-content/uploads/sites/2/2023/07/Quiz-UFO_052721_AP_Michael-Sohn.jpg?w=900',
   'source_id': 'thehill',
   'source_priority': 393,
   'country': ['united states of america'],
   'category': ['politics'],
   'language': 'english'},
  {'article_id': '4c58c55da39c31d52e6d1abb5818d29d',
   'title': "China says it 'deplores' US military transfer to Taiwan",
   'link': 'https://thehill.com/policy/defense/4182023-china-says-it-deplores-us-military-transfer-to-taiwan/',
   'keywords': ['Defense', 'International'],
   'creator': ['Ellen Mitchell'],
   'video_url': None,
   'description': 'China is lashing out at the Biden administration’s approval of the first-ever U.S. military transfer to Taiwan using a program usually saved for sovereign nations. Beijing, which views Taipei as its own territory and has repeatedly threatened to bring it under its control using force, on Thursday claimed the U.S. transfer “severely violates the one-China principle.” “This...',
   'content': 'China is lashing out at the Biden administration’s approval of the first-ever U.S. military transfer to Taiwan using a program usually saved for sovereign nations. Beijing, which views Taipei as its own territory and has repeatedly threatened to bring it under its control using force, on Thursday claimed the U.S. transfer “severely violates the one-China principle.” “This severely violates the one-China principle and the stipulations of the three China-U.S. joint communiques,” China’s Foreign Ministry spokesman Wang Wenbin said at a news conference in Beijing. “China deplores and firmly opposes it.” The State Department on Tuesday notified Congress it would sell Taiwan an as part of the department’s foreign military financing (FMF) program, which uses U.S. taxpayer dollars to fund the supply of materials to foreign countries. The package is meant to “strengthen Taiwan’s self-defense capabilities,” according to the agency. This marks the first time the U.S. has provided military assistance under FMF to Taiwan and the second time it’s given it to a non-nation-state, the first being to the African Union. Washington has previously sold Taiwan weapons under its Foreign Military Sale program, which doesn’t imply statehood, though U.S. officials said this new method of weapons transfer does not mean a change in policy. But China, which in the past has strongly protested any and all U.S. defense aid to the independently governed island, on Thursday urged Washington to “stop creating tensions across the Taiwan Strait” by “enhancing U.S.-Taiwan military connections and arming Taiwan,” according to Wenbin. It’s unknown what exact weapons and equipment will be in the military package, but it could include air and coastal defense systems, armed vehicles, ballistic missile and cyber defenses, ammunition or even training support for Taiwanese military forces. Lawmakers applauded the package, including Senate Foreign Relations Committee Chairman Bob Menendez (D-N.J.), who called the move a “meaningful contribution” on the part of the administration. “In the face of increasingly aggressive People’s Republic of China military actions in the [Taiwan Strait], the United States must move quickly to provide support for Taiwan’s defense,” Menendez said in a statement. And Rep. Michael McCaul (R-Texas), the chair of the House Foreign Affairs Committee, said the weapons to Taiwan will help the island “protect other democracies in the region” and “strengthen the U.S. deterrence posture and ensure our national security from an increasingly aggressive [Chinese Communist Party].”',
   'pubDate': '2023-08-31 21:36:26',
   'image_url': 'https://thehill.com/wp-content/uploads/sites/2/2023/07/AP23207120236485-e1690582692588.jpg?w=900',
   'source_id': 'thehill',
   'source_priority': 393,
   'country': ['united states of america'],
   'category': ['politics'],
   'language': 'english'}
 ]
}


newspaper_articles_data = rename_and_select_columns_news(json_news_to_df(raw_newspaper_data))
processed_article = process_articles(word_by_grade_level_data=word_by_grade_level_data, newspaper_articles_data=newspaper_articles_data)
processed_article.to_csv('processed_article', index=False)