"""인프라/DevOps 채용공고 더미 생성기"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from generator_base import CategoryConfig, run_category_generator, parse_args

CONFIG = CategoryConfig(
    name="devops",
    display_name="인프라/DevOps",
    search_queries=[
        "사람인 DevOps SRE 인프라 채용공고 2026 Kubernetes AWS",
        "원티드 클라우드 인프라 엔지니어 채용 2026",
        "2026 DevOps SRE 인기 기술스택 트렌드 Kubernetes Terraform",
        "2026 클라우드 DevOps 엔지니어 연봉 복지",
    ],
    job_titles=[
        "DevOps 엔지니어",
        "SRE (Site Reliability Engineer)",
        "클라우드 인프라 엔지니어",
        "Kubernetes 엔지니어",
        "플랫폼 엔지니어",
        "CI/CD 파이프라인 엔지니어",
        "AWS 클라우드 아키텍트",
        "인프라 자동화 엔지니어",
    ],
    skills_hint=[
        "Kubernetes", "Docker", "Terraform", "Ansible", "Helm",
        "AWS", "GCP", "Azure", "CI/CD", "Jenkins", "GitHub Actions",
        "GitLab CI", "ArgoCD", "Prometheus", "Grafana", "ELK Stack",
        "Linux", "Bash", "Python", "Nginx", "Istio",
    ],
)


if __name__ == "__main__":
    args = parse_args("인프라/DevOps 더미 채용공고 생성")
    run_category_generator(CONFIG, count=args.count, use_cache=args.use_cache)
