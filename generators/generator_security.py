"""보안 채용공고 더미 생성기"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from generator_base import CategoryConfig, run_category_generator, parse_args

CONFIG = CategoryConfig(
    name="security",
    display_name="보안",
    search_queries=[
        "사람인 정보보안 클라우드보안 채용공고 2026",
        "원티드 보안 엔지니어 DevSecOps 채용 2026",
        "2026 정보보안 클라우드보안 인기 기술스택 트렌드",
        "2026 보안 전문가 연봉 복지 침투테스트",
    ],
    job_titles=[
        "정보보안 엔지니어",
        "클라우드 보안 엔지니어",
        "DevSecOps 엔지니어",
        "침투테스트 전문가 (Pentester)",
        "보안 아키텍트",
        "애플리케이션 보안 엔지니어",
        "SOC 분석가",
        "취약점 분석 엔지니어",
    ],
    skills_hint=[
        "Python", "Linux", "네트워크 보안", "SIEM", "WAF",
        "Burp Suite", "Metasploit", "Nmap", "Wireshark",
        "AWS Security", "Zero Trust", "ISO 27001", "ISMS",
        "취약점 분석", "침투테스트", "DevSecOps", "OWASP",
        "CSPM", "SAST", "DAST", "Docker", "Kubernetes 보안",
    ],
)


if __name__ == "__main__":
    args = parse_args("보안 더미 채용공고 생성")
    run_category_generator(CONFIG, count=args.count, use_cache=args.use_cache)
