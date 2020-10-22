from typing import Dict
from google.ads.google_ads.client import GoogleAdsClient
import pandas as pd

_ADS_URL = "https://googleads.googleapis.com/v5/customers/"

"""
Можно использовать разные способы загрузки авторизации, мне удобен словарь 
В словаре должны быть
developer_token - токен разработника. Его можно взять в интерфейсе Googe Ads MCC - Настройки - Центр API
access_token и refresh_token можно взять с помощью специального запроса
client_id и client_secret можно взять в console.google.com
login_customer_id - идентификатор родительского аккаунта. 
Опциональный параметр. Необходим, если хотите снять статистику дочернего аккаунта

Подробнее об авторизации в Googel Ads
https://developers.google.com/adwords/api/docs/guides/authentication
"""
auth_data = {
    'developer_token': '',
    'access_token': "",
    'refresh_token': '',
    'client_id': '',
    'client_secret': '',
    'login_customer_id': None
}


class GoogleAds(object):

    def __init__(self, credentials: Dict):
        """
        :param credentials: Данные авторизации
        """

        self.credentials = credentials
        self.client = GoogleAdsClient.load_from_dict(self.credentials)

    def get_service(self):
        """
        Сервисный объект для доступа к Google API.
        """

        return self.client.get_service("GoogleAdsService", version="v5")

    @staticmethod
    def get_campaigns(client, service, customer_id: str) -> Dict:
        """
        Возвращает словарь с кампаниями

        :param client: клиент Google Ads

        :param service: сервисный объект

        :param customer_id: идентификатор аккаунта Google Ads
        :type customer_id: str

        :rtype: dict
        :return: словарь с информацией о кампаниях
        """

        query = """
                SELECT 
                    campaign.id, 
                    campaign.name,
                    campaign.status,
                    campaign.advertising_channel_type
                FROM campaign
                ORDER BY campaign.id
                """

        result = {'id': [], 'name': [], 'status': [], 'type': []}

        response = service.search_stream(customer_id, query=query)

        advertiser_channel_type_enum = client.get_type('AdvertisingChannelTypeEnum')
        get_campaign_type = advertiser_channel_type_enum.AdvertisingChannelType.Name

        campaign_status_enum = client.get_type('CampaignStatusEnum')
        get_campaign_status = campaign_status_enum.CampaignStatus.Name

        for batch in response:
            for row in batch.results:
                row_type = get_campaign_type(row.campaign.advertising_channel_type)
                row_status = get_campaign_status(row.campaign.status)

                result['name'].append(row.campaign.name)
                result['id'].append(row.campaign.id)
                result['status'].append(row_status)
                result['type'].append(row_type)

        return result

    @staticmethod
    def get_adgroups(service, customer_id: str, campaign_id=None) -> Dict:
        """
        Выгрузка справочника групп объявлений

        :param service: сервисный объект
        :param customer_id: идентификатор рекламного кабинета Google Ads (без разделителей: 123456786, а не 123-456-789)
        :param campaign_id: идентификатор кампании. Если не указан, снимаются все группы объявлений кабинета
        :return: Dict
        """
        query = """
            SELECT 
                campaign.id, 
                ad_group.id, 
                ad_group.name
            FROM ad_group"""

        if campaign_id:
            query = "%s WHERE campaign.id = %s" % (query, campaign_id)

        results = {'campaign_id': [], 'id': [], 'name': []}

        response = service.search_stream(customer_id, query=query)

        for batch in response:
            for row in batch.results:
                results['campaign_id'].append(row.campaign.id)
                results['id'].append(row.ad_group.id)
                results['name'].append(row.ad_group.name)

        return results

    @staticmethod
    def get_ads(service, customer_id: str, campaign_id=None) -> Dict:
        """
        Возвращает справочник объявлений

        :param service: сервисный объект
        :param customer_id: идентификатор рекламного кабинета Google Ads (без разделителей: 123456786, а не 123-456-789)
        :param campaign_id: идентификатор кампании. Если не указан, снимаются все группы объявлений кабинета
        :return: Dict
        """
        query = """
                SELECT 
                    campaign.id, 
                    ad_group.id, 
                    ad_group_ad.ad.id
                FROM ad_group_ad
                """

        if campaign_id:
            query = "%s WHERE campaign.id = %s" % (query, campaign_id)

        results = {'campaign_id': [], 'ad_group_id': [], 'id': []}

        response = service.search_stream(customer_id, query=query)

        for batch in response:
            for row in batch.results:
                results['campaign_id'].append(row.campaign.id)
                results['ad_group_id'].append(row.ad_group.id)
                results['id'].append(row.ad_group_ad.ad.id)

        return results

    @staticmethod
    def get_campaign_report(service, customer_id: str, date_stat: str) -> Dict:
        """
        Возвращает отчет об эффективноти рекламных кампаний за указанную дату
        :param service: сервисный объект
        :param customer_id: идентификатор рекламного кабинета Google Ads (без разделителей: 123456786, а не 123-456-789)
        :param date_stat: дата, за которую нужно снять статистику
        :return: отчет об эффективноти рекламных кампаний за указанную дату
        """
        query = """
            SELECT 
                campaign.id, 
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.engagements,
                metrics.interactions
            FROM campaign
            WHERE 
                segments.date BETWEEN '{dt}' AND '{dt}'
                AND
                metrics.impressions > 0
            ORDER BY campaign.id
            """.format(dt=date_stat)

        result = {'date': [],
                  'id': [],
                  'impressions': [],
                  'clicks': [],
                  'cost_micros': [],
                  'engagements': [],
                  'interactions': []}

        response = service.search_stream(customer_id, query=query)

        for batch in response:
            for row in batch.results:
                result['date'].append(date_stat)
                result['id'].append(row.campaign.id)
                result['impressions'].append(row.metrics.impressions)
                result['clicks'].append(row.metrics.clicks)
                result['cost_micros'].append(row.metrics.cost_micros)
                result['engagements'].append(row.metrics.engagements)
                result['interactions'].append(row.metrics.interactions)

        return result

    @staticmethod
    def get_adgroup_report(service, customer_id: str, date_stat: str) -> Dict:
        """
        Возвращает отчет об эффектиности групп объявлеий за указанную дату

        :param service: сервисный объект
        :param customer_id: идентификатор рекламного кабинета Google Ads (без разделителей: 123456786, а не 123-456-789)
        :param date_stat: дата, за которую нужно снять статистику
        :return: отчет об эффективноти рекламных кампаний за указанную дату
        """
        query = """
            SELECT 
                campaign.id,
                ad_group.id, 
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.engagements,
                metrics.interactions
            FROM ad_group
            WHERE 
                segments.date BETWEEN '{dt}' AND '{dt}'
                AND
                metrics.impressions > 0
            ORDER BY ad_group.id""".format(dt=date_stat)

        result = {'date': [],
                  'id': [],
                  'campaign_id': [],
                  'impressions': [],
                  'clicks': [],
                  'cost_micros': [],
                  'engagements': [],
                  'interactions': []}

        response = service.search_stream(customer_id, query=query)

        for batch in response:
            for row in batch.results:

                result['date'].append(date_stat)
                result['id'].append(row.ad_group.id)
                result['campaign_id'].append(row.campaign.id)
                result['impressions'].append(row.metrics.impressions)
                result['clicks'].append(row.metrics.clicks)
                result['cost_micros'].append(row.metrics.cost_micros)
                result['engagements'].append(row.metrics.engagements)
                result['interactions'].append(row.metrics.interactions)

        return result

    @staticmethod
    def get_ads_report(service, customer_id: str, date_stat: str) -> Dict:
        """
        Возвращает отчет об эффектиности объявлеий за указанную дату

        :param service: сервисный объект
        :param customer_id: идентификатор рекламного кабинета Google Ads (без разделителей: 123456786, а не 123-456-789)
        :param date_stat: дата, за которую нужно снять статистику
        :return: отчет об эффективноти рекламных кампаний за указанную дату
        """
        query = """
            SELECT 
                campaign.id,
                ad_group.id, 
                ad_group_ad.ad.id,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.engagements,
                metrics.interactions
            FROM ad_group_ad
            WHERE 
                segments.date BETWEEN '{dt}' AND '{dt}'
                AND
                metrics.impressions > 0
            ORDER BY ad_group_ad.ad.id""".format(dt=date_stat)

        result = {'date': [],
                  'adgroup_id': [],
                  'campaign_id': [],
                  'id': [],
                  'impressions': [],
                  'clicks': [],
                  'cost_micros': [],
                  'engagements': [],
                  'interactions': []}

        response = service.search_stream(customer_id, query=query)

        for batch in response:
            for row in batch.results:

                result['date'].append(date_stat)
                result['id'].append(row.ad_group_ad.ad.id)
                result['adgroup_id'].append(row.ad_group.id)
                result['campaign_id'].append(row.campaign.id)
                result['impressions'].append(row.metrics.impressions)
                result['clicks'].append(row.metrics.clicks)
                result['cost_micros'].append(row.metrics.cost_micros)
                result['engagements'].append(row.metrics.engagements)
                result['interactions'].append(row.metrics.interactions)

        return result


if __name__ == "__main__":

    ads = GoogleAds(auth_data)
    ads_service = ads.get_service()

    ads_id = ""  # Идентификатор рекламного кабинета

    # Получаем справочник кампаний
    campaigns = ads.get_campaigns(client=ads.client, service=ads_service, customer_id=ads_id)
    campaigns_df = pd.DataFrame(campaigns)
    print(campaigns_df)

    # Отчет за 1 сентября 2020

    report = ads.get_campaign_report(ads_service, customer_id=ads_id, date_stat="2020-09-01")
    report_df = pd.DataFrame(report)
    print(report_df)
