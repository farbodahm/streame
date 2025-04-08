# Why We Chose ETCD for Node Discovery

For our distributed system **Streame**, we evaluated several options for node discovery, including **ETCD**, **ZooKeeper**, and **Consul**. After careful consideration, we decided to go with **ETCD**. Here's why:

## 1. Native Kubernetes Integration
Streame is designed to run on **Kubernetes**, and ETCD is the backbone of Kubernetes itself. Choosing ETCD eliminates the need to deploy and manage an additional discovery service like ZooKeeper or Consul. This simplifies our operational overhead and makes the system leaner and more maintainable.

## 2. CNCF Ecosystem Alignment
Our stack already heavily leverages **CNCF projects** like **gRPC** and **NATS**. ETCD is a graduated CNCF project, making it a natural choice for staying within a trusted and interoperable ecosystem. This increases confidence in long-term support and community-driven innovation.

## 3. Simplicity and Ease of Use
ETCD is known for its simplicity, both in setup and in usage. It has a straightforward API and minimal configuration requirements.

---

Let us know if you think there's something we missed or should consider in the future.
