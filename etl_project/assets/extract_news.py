import requests

class News:

    def __init__(self, api_key: str):
        self.base_url = 'https://newsdata.io/api/1/'
        self.api_key = api_key
        if api_key is None:
            raise Exception('Please enter a valid API key. A valid key cannot be None.')
        
    def _build_params(self, param:str, value) -> dict:

        str_params = {'q','qInTitle','qInMeta','country','category','language','domain','domainurl','excludedomain', 'prioritydomain','timezone'}
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
    
    def get_news(
            self, which_news = 'news', q:str=None, qInTitle:str=None, qInMeta:str=None, timeframe:int=None, country:[str]=None, category:[str]=None, 
            language:[str]='en', domain:[str]=None, domainurl:[str]=None, excludedomain:[str]=None, prioritydomain:str=None, 
            timezone:str=None, full_content:bool=None, image:bool=None, video:bool=None, size:int=10
        )->dict:

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
        
        if which_news not in {'news','crypto','archive'}:
            raise Exception("Please choose a valid API endpoint. Options: 'latest','crypto','archive'")

        base_url = self.base_url + which_news

        url_params = {
            'q':q, 'qInTitle':qInTitle, 'qInMeta':qInMeta, 'timeframe':timeframe, 'country':country, 
            'category':category, 'language':language, 'domain':domain, 'domainurl':domainurl, 'excludedomain':excludedomain, 
            'prioritydomain':prioritydomain, 'timezone':timezone, 'full_content':full_content, 'image':image, 'video':video, 'size':size
        }

        params = {}
        for key, value in url_params.items():
            if value is not None:
                url_params.update(self._build_params(param = key, value = value))

        headers = {'X-ACCESS-KEY': self.api_key}
        
        response = requests.get(url = base_url, params = params, headers = headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Request to {base_url} failed. Status code: {response.status_code} Response: {response.text}")
        
    def transform_news():
        pass

    def load_news():
        pass