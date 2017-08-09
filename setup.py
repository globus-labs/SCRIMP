from setuptools import setup
import scrimp

readme_text = ''
with open('README.md', 'r') as f:
    readme_text = f.read()

setup(
    name='scrimp',
    version='0.1.0',

    install_requires=['sqlalchemy', 'psycopg2', 'boto', 'pytz'],
    packages=['scrimp',
              'scrimp.cloud', 'scrimp.cloud.aws', 'scrimp.cloud.simaws',
              'scrimp.scheduler', 'scrimp.scheduler.condor', 'scrimp.scheduler.simfile'],
    package_data={'': ['*.ini']},
    entry_points={'console_scripts':
                  ['scrimp = scrimp.cli:main']},

    long_description=readme_text
)
