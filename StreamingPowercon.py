from constants import POWERCON_CREDENTIALS, TARGET_CREDENTIALS
from stream import RTStreamingPowerCon

if __name__ == '__main__':
    RTStreamingPowerCon(POWERCON_CREDENTIALS, TARGET_CREDENTIALS)