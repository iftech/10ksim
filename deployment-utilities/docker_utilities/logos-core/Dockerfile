# AMD64 Dockerfile for logos-core DST testing
FROM debian:bookworm-slim AS builder

ARG NIX_USER=nixuser

# Standard tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    build-essential ca-certificates \
    libssl3 \
    iproute2 \
    procps \
    ca-certificates \
    gcc-multilib \
    g++-multilib \
    libc6-dev-i386 \
    ssh \
    cmake

ENV PATH=/usr/local/bin:$PATH

RUN git config --global http.sslVerify false

# logos-core dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl xz-utils && \
    useradd -m nixuser && \
    mkdir /nix && \
    chown nixuser /nix && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add config for nix and trust "nixuser" as root before we switch users.
RUN mkdir -p /etc/nix/
RUN cat >> /etc/nix/nix.conf << 'EOF'
experimental-features = nix-command flakes
accept-flake-config = true
trusted-substituters = https://nix-cache.status.im/
trusted-public-keys = nix-cache.status.im-1:x/93lOfLU+duPplwMSBR+OlY4+mo+dCN7n0mr4oPwgY= cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=
trusted-users = root nixuser
filter-syscalls = false
EOF
# RUN chown -R nixuser /home/nixuser/.config

RUN mkdir -p /home/nixuser && chown nixuser:nixuser /home/nixuser
USER ${NIX_USER}
ENV USER=${NIX_USER}
WORKDIR /home/nixuser
ENV HOME=/home/nixuser

RUN curl -L https://nixos.org/nix/install | sh -s -- --no-daemon
ENV PATH="$HOME/.nix-profile/bin:${PATH}"
ENV NIX_USER_PROFILE_DIR="/nix/var/nix/profiles/per-user/$USER"

# Create a place for our binaries (lgpm, etc)
RUN mkdir -p "$HOME/bin"
ENV PATH="$HOME/bin:$PATH"
RUN cd $HOME

WORKDIR $HOME
RUN git clone https://github.com/logos-co/logos-package-manager.git
WORKDIR $HOME/logos-package-manager
RUN git checkout tutorial-v1
RUN nix build
RUN ln -s $(realpath ./result)/bin/lgpm $HOME/bin/lgpm

WORKDIR $HOME
RUN git clone https://github.com/logos-co/nix-bundle-lgx.git
WORKDIR $HOME/nix-bundle-lgx
RUN git checkout tutorial-v1

WORKDIR $HOME
RUN git clone https://github.com/logos-co/logos-delivery-module.git
WORKDIR $HOME/logos-delivery-module
RUN git checkout v0.1.1
RUN # You have to use dual for some reason.
RUN nix bundle --bundler $HOME/nix-bundle-lgx#dual .
RUN lgpm install --file logos-delivery_module-module-lgx-*/logos-delivery_module-module.lgx --modules-dir "$HOME/.logos/modules"

WORKDIR $HOME
RUN git clone https://github.com/logos-co/logos-logoscore-cli.git
WORKDIR $HOME/logos-logoscore-cli
RUN git checkout tutorial-v1
RUN nix build
RUN ln -s $(realpath ./result)/bin/logoscore $HOME/bin/logoscore

WORKDIR $HOME
COPY entrypoint.sh entrypoint.sh
ENTRYPOINT ["/bin/sh", "-c", "./entrypoint.sh"]