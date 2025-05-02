from setuptools import setup, find_packages

setup(
    name="tap-google-ad-manager",
    version="0.1.0",
    description="A Singer tap for Google Ad Manager.",
    author="The Daily Upside",
    author_email="dev@thedailyupside.com",
    packages=find_packages(),
    install_requires=[
        "singer-sdk>=0.13.0",
        "requests>=2.25.1",
        "google-auth>=2.0.0",
    ],
    entry_points={
        "console_scripts": [
            "tap-google-ad-manager=tap_google_ad_manager.tap:TapGoogleAdManager.cli",
        ],
    },
)