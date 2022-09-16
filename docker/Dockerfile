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

# Create downloader user and working directory
RUN addgroup --gid ${HOST_GID} downloader \
  && useradd --uid ${HOST_UID} --gid ${HOST_GID} -ms /bin/bash downloader \
  && echo "downloader ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/downloader \
  && chmod 0440 /etc/sudoers.d/downloader \
  && export ENV_NAME="$ENV_NAME" \
  && mkdir -p /opt/conda /opt/services/logs /opt/services/downloader_app/ \
  && chmod -R a+rwx /opt/conda /opt/services/downloader_app/ /tmp \
  && chown downloader:downloader /opt/services/ /tmp \
  && echo 'source /opt/conda/bin/activate "$ENV_NAME" && exec "$@"' > /activate.sh \
  && echo 'source activate "$ENV_NAME"' >  /home/downloader/.bashrc \
  && chmod +x /activate.sh

# Copy dependencies file
COPY --chown=downloader:downloader conda/dev.yaml /tmp/environment.yaml
COPY --chown=downloader:downloader downloader_app /opt/services/downloader_app/
COPY --chown=downloader:downloader docker/celery/run.sh  /opt/services/celery-run.sh
# COPY --chown=downloader:downloader docker/ee/credentials /home/downloader/.config/earthengine/credentials
# COPY --chown=downloader:downloader docker/ee/mycreds.txt  /opt/services/downloader_app/mycreds.txt
COPY --chown=downloader:downloader env.tpl pyproject.toml poetry.lock README.md /opt/services/
COPY --chown=downloader:downloader docker/scripts/poetry-install.sh /tmp/poetry-install.sh

ENV PATH "$PATH:/home/downloader/.local/bin"
ENV PATH /opt/conda/envs/$ENV_NAME/bin:$PATH

USER downloader

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

WORKDIR /opt/services/downloader_app/

ENTRYPOINT ["bash", "/activate.sh"]
CMD ["/opt/services/celery-run.sh"]
