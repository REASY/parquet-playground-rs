import random
import time
from datetime import datetime, timedelta
import gzip
import json

possible_tag_keys = [
    "cloud.provider",
    "cloud.region",
    "cloud.availability_zone",
    "cloud.account.id",
    "cloud.project.id",
    "cloud.platform",
    
    "k8s.cluster.name",
    "k8s.namespace.name",
    "k8s.node.name",
    "k8s.pod.name",
    "k8s.container.name",
    "k8s.cronjob.name",
    "k8s.deployment.name",
    "k8s.service.name",
    "k8s.daemonset.name",
    "k8s.statefulset.name",
    "k8s.job.name",

    "os.type",
    "os.version",

    "service.name",
    "service.version",
    "service.instance.id",

    "env",
    "tenant.id",
    "application",
    "stack",
    "stage",

    "region",
    "availability_zone",
    "instance.type",
    "instance.id",
    
    "host.name",
    "host.ip",
    "host.arch",
    "host.os",

    "build.number",
    "commit.sha",
    "git.branch",
    "git.repository_url",

    "team.name",
    "owner",
]

def generate_random_tag_value(tag_key):
    """
    Given a tag key, returns a reasonable random value 
    consistent with real-world usage (where possible).
    Otherwise, falls back to a generic value.
    """

    if tag_key == "cloud.provider":
        return random.choice(["aws", "azure", "gcp", "on-prem"])
    elif tag_key == "cloud.region":
        return random.choice(["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"])
    elif tag_key == "cloud.availability_zone":
        # For AWS, AZ is typically region + letter
        region = random.choice(["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"])
        letter = random.choice(["a", "b", "c"])
        return f"{region}{letter}"
    elif tag_key == "cloud.account.id":
        return f"{random.randint(100000000000, 999999999999)}"
    elif tag_key == "cloud.project.id":
        return f"project-{random.randint(1000, 9999)}"
    elif tag_key == "cloud.platform":
        return random.choice(["eks", "ec2", "lambda", "ecs"])

    elif tag_key == "k8s.cluster.name":
        return random.choice(["prod-cluster", "dev-cluster", "staging-cluster", "testing-cluster"])
    elif tag_key == "k8s.namespace.name":
        return random.choice(["default", "production", "staging", "kube-system", "monitoring"])
    elif tag_key == "k8s.node.name":
        return f"ip-{random.randint(10,99)}-{random.randint(0,255)}-{random.randint(0,255)}-{random.randint(0,255)}"
    elif tag_key == "k8s.pod.name":
        return f"myapp-pod-{random.randint(1, 9999)}"
    elif tag_key == "k8s.container.name":
        return random.choice(["frontend", "backend", "worker", "sidecar", "init-container"])
    elif tag_key == "k8s.cronjob.name":
        return random.choice(["nightly-job", "weekly-report", "db-cleanup"])
    elif tag_key == "k8s.deployment.name":
        return random.choice(["web-deployment", "api-deployment", "worker-deployment"])
    elif tag_key == "k8s.service.name":
        return random.choice(["web-service", "api-service", "internal-service"])
    elif tag_key == "k8s.daemonset.name":
        return random.choice(["logging-daemon", "monitoring-daemon", "security-daemon"])
    elif tag_key == "k8s.statefulset.name":
        return random.choice(["db-statefulset", "cache-statefulset", "es-statefulset"])
    elif tag_key == "k8s.job.name":
        return random.choice(["db-migration-job", "batch-processing-job", "cleanup-job"])

    elif tag_key == "os.type":
        return random.choice(["linux", "windows", "darwin"])
    elif tag_key == "os.version":
        return f"{random.randint(1, 10)}.{random.randint(0, 15)}"
    
    elif tag_key == "service.name":
        return random.choice(["payment-service", "user-service", "order-service", "notification-service"])
    elif tag_key == "service.version":
        return f"v{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}"
    elif tag_key == "service.instance.id":
        return f"instance-{random.randint(1000,9999)}"
    
    elif tag_key == "env":
        return random.choice(["dev", "staging", "prod"])
    elif tag_key == "tenant.id":
        return f"tenant-{random.randint(100,999)}"
    elif tag_key == "application":
        return random.choice(["myapp", "inventory", "accounts", "webportal"])
    elif tag_key == "stack":
        return random.choice(["frontend", "backend", "cache", "worker"])
    elif tag_key == "stage":
        return random.choice(["development", "qa", "production", "canary"])
    
    elif tag_key == "region":
        # Generic region tag
        return random.choice(["us-east", "us-west", "eu-central", "ap-east"])
    elif tag_key == "availability_zone":
        # Generic availability zone
        return random.choice(["zone-a", "zone-b", "zone-c"])
    elif tag_key == "instance.type":
        return random.choice(["t2.micro", "m5.large", "c5.xlarge", "r5.2xlarge"])
    elif tag_key == "instance.id":
        return f"i-{random.randint(100000,999999)}"
    
    elif tag_key == "host.name":
        return f"ec2-{random.randint(100,999)}-{random.randint(1000,9999)}"
    elif tag_key == "host.ip":
        return f"{random.randint(10,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
    elif tag_key == "host.arch":
        return random.choice(["x86_64", "arm64"])
    elif tag_key == "host.os":
        return random.choice(["linux", "windows", "amazon-linux", "ubuntu", "redhat"])
    
    elif tag_key == "build.number":
        return str(random.randint(1000, 9999))
    elif tag_key == "commit.sha":
        # A simulated commit SHA
        return f"{random.randint(1, 10**7):x}"
    elif tag_key == "git.branch":
        return random.choice(["main", "master", "develop", "feature/login", "hotfix/payment"])
    elif tag_key == "git.repository_url":
        return random.choice([
            "https://github.com/myorg/myapp",
            "https://bitbucket.org/company/app-repo",
            "https://gitlab.com/example/project"
        ])
    
    elif tag_key == "team.name":
        return random.choice(["TeamA", "TeamB", "TeamC", "PlatformTeam", "OpsTeam"])
    elif tag_key == "owner":
        return random.choice(["alice", "bob", "charlie", "david", "eve"])

    # Add more specifically handled keys here if desired.

    # Fallback: generic tag value
    return f"value_{random.randint(1, 999999)}"

def generate_histograms(N, M, seed=42):
    """
    Generate N histograms.
    Each histogram has:
     - M random (tag, value) pairs (strings).
     - An array of 60 data points with cumulative count & sum.
    """
    # Set the random seed for reproducibility
    random.seed(seed)
    
    histograms = []

    # bound M so later `random.sample(possible_tag_keys, k=M)` does not fail
    M = min(M, len(possible_tag_keys))
    
    # Choose a base timestamp (in ms). For example, "now".
    start_time_ms = int(time.time() * 1000)
    
    for i in range(N):
        chosen_tag_keys = random.sample(possible_tag_keys, k=M)

        # Generate M randomized tags (key-value pairs)
        tagValues = []
        for key in chosen_tag_keys:
            tagValues.append(generate_random_tag_value(key))
        

        
        # Initialize cumulative counters
        cumulative_count = 0
        cumulative_sum = 0
        cumulative_sum_double = 0.0
        
        # Each data point is offset by 1 second (1000 ms) from the previous
        current_time_ms = start_time_ms

        # Prepare the 60 data points
        ts_list = []
        sums_long_list = []
        sums_double_list = []
        count_list = []

        is_long = random.choice([True, False])
        
        for j in range(60):
            increment_count = random.randint(1, 10)
            cumulative_count += increment_count

            if is_long:
              increment_sum = random.randint(1, 50)
              cumulative_sum += increment_sum
              sums_long_list.append(cumulative_sum)
            else:
              increment_sum_double = random.uniform(0.01, 100)
              cumulative_sum_double += increment_sum_double
              sums_double_list.append(cumulative_sum_double)

            ts_list.append(current_time_ms)
            count_list.append(cumulative_count)
            
            # Move to the next ts
            current_time_ms += 100
        
        histograms.append({
            "tags": chosen_tag_keys.copy(),
            "tagValues": tagValues,
            "ts": ts_list,
            "sumsDouble": sums_double_list,
            "sumsLong": sums_long_list,
            "count": count_list
        })
    
    return histograms

def get_metric(name: str, hist: object) -> object:
  metric = {
    "metric": "test",
    "series": hist
  }
  return metric


def write_metric(metric: object, path: str):
  with gzip.open(path, 'wt', encoding="ascii") as zipfile:
       json.dump(metric, zipfile)

if __name__ == "__main__":
    N = 1000  # Number of histograms
    M = 50  # Number of (tag, value) pairs per histogram
    hist = generate_histograms(N, M, seed=42)
    metric = get_metric("test", hist)
    print(f"Generated metric with {len(hist)} series")
    write_metric(metric, "test.json.gz")
    print("Wrote metric to compressed JSON file")