FROM python:latest

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip


# RUN apt-get update
RUN apt-get -y install sudo

# RUN apt-get update
RUN apt-get install ca-certificates curl gnupg -y
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN chmod a+r /etc/apt/keyrings/docker.gpg

# Add the repository to Apt sources:
RUN echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN  apt-get update
RUN apt-get -y install docker-ce-cli


ENV USER=theuser
RUN adduser --home /home/$USER --disabled-password --gecos GECOS $USER \
  && echo "$USER ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/$USER \
  && chmod 0440 /etc/sudoers.d/$USER \
  && groupadd docker \
  && usermod -aG docker $USER \
  && chsh -s /bin/zsh $USER
USER $USER

ENV HOME=/home/$USER

# Set the working directory
WORKDIR /load_balancer/

COPY requirements.txt /load_balancer/

RUN pip install -r requirements.txt

COPY . /load_balancer/

# RUN chmod +x main.py
CMD ["python3", "main.py"]