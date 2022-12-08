FROM condaforge/mambaforge

ARG HOST_UID
ARG HOST_GID
ARG ENV
ENV ENV_NAME=env-satellite
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -qq update --yes \
  && apt-get -qq install --yes --no-install-recommends \
  build-essential git make postgresql-client vim \
  ca-certificates wget locales cron sudo \
  && rm -rf /var/lib/apt/lists/*

#Set locale
RUN sed -i -e "s/# pt_BR.*/pt_BR.UTF-8 UTF-8/" /etc/locale.gen \
    && dpkg-reconfigure --frontend=noninteractive locales \
    && update-locale LANG=pt_BR.UTF-8

# Create satellite user and working directory
RUN addgroup --gid ${HOST_GID} satellite \
  && useradd --uid ${HOST_UID} --gid ${HOST_GID} -ms /bin/bash satellite \
  && echo "satellite ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/satellite \
  && chmod 0440 /etc/sudoers.d/satellite \
  && export ENV_NAME="$ENV_NAME" \
  && mkdir -p /opt/conda /opt/services/logs /opt/services/satellite_weather_downloader/ \
  && chmod -R a+rwx /opt/conda /opt/services/satellite_weather_downloader/ /tmp \
  && chown satellite:satellite /opt/services/ /tmp \
  && echo 'source /opt/conda/bin/activate "$ENV_NAME" && exec "$@"' > /activate.sh \
  && echo 'source activate "$ENV_NAME"' >  /home/satellite/.bashrc \
  && chmod +x /activate.sh

# Copy dependencies file
COPY --chown=satellite:satellite conda/dev.yaml /tmp/environment.yaml
COPY --chown=satellite:satellite satellite_weather_downloader /opt/services/satellite_weather_downloader/
COPY --chown=satellite:satellite docker/scripts/run_celery.sh  /opt/services/celery-run.sh
COPY --chown=satellite:satellite env.tpl pyproject.toml poetry.lock README.md /opt/services/
COPY --chown=satellite:satellite docker/scripts/poetry-install.sh /tmp/poetry-install.sh

ENV PATH "$PATH:/home/satellite/.local/bin"
ENV PATH /opt/conda/envs/$ENV_NAME/bin:$PATH

USER satellite

# Use environment to create the env-satellite
RUN mamba env create -n $ENV_NAME \
    --file /tmp/environment.yaml \
  && cd /opt/services/ \
  && . /tmp/poetry-install.sh \
  && conda clean --all \
  && find /opt/conda/ -type f,l -name '*.a' -delete \
  && find /opt/conda/ -type f,l -name '*.pyc' -delete \
  && find /opt/conda/ -type f,l -name '*.js.map' -delete \
  && rm -rf /opt/conda/pkgs

WORKDIR /opt/services/satellite_weather_downloader/

ENTRYPOINT ["bash", "/activate.sh"]
CMD ["/opt/services/celery-run.sh"]
