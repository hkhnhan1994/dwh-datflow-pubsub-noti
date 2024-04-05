
from google.cloud import pubsub_v1
import logging

print = logging.info

def getTopic(
    project: str,
    Topic_name:str):
    publisher = pubsub_v1.PublisherClient()
    topic_path=publisher.topic_path(project, Topic_name)
    print(" topic: {} in project {}".format(project, topic_path))
    return topic_path
def getSub(project: str, Subscriber_name: str):
        '''Get Subscriber from configuration.'''
        Subscriber = pubsub_v1.SubscriberClient()
        subscription_path=Subscriber.subscription_path(project, Subscriber_name)
        # print(" subscription_path: {}".format(subscription_path))
        try:
            exist_Subscriber=Subscriber.get_subscription(request={"subscription": subscription_path})
            # print(f"sub {Subscriber_name} found: {_exist_Subscriber}")
            return  exist_Subscriber
        except Exception as e:
             logging.debug(e)
             return None
