import requests

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