FROM jonasvautherin/px4-gazebo-headless:latest

# FIRMWARE_DIR, WORKSPACE_DIR, SITL_RTSP_PROXY are already set by the base image.
# We override only the entrypoint so all PX4 binaries and models stay identical.

COPY entrypoint.sh /root/entrypoint-swarm.sh
RUN chmod +x /root/entrypoint-swarm.sh

ENTRYPOINT ["/root/entrypoint-swarm.sh"]
