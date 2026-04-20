"""AI/ML 채용공고 더미 생성기"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from generator_base import CategoryConfig, run_category_generator, parse_args

CONFIG = CategoryConfig(
    name="ai_ml",
    display_name="AI/ML",
    search_queries=[
        "사람인 AI ML 엔지니어 채용공고 2026 LLM RAG",
        "원티드 머신러닝 딥러닝 채용 2026",
        "2026 AI ML 엔지니어 인기 기술스택 트렌드 LLM MLOps",
        "2026 AI 개발자 연봉 복지 LLM 파인튜닝",
    ],
    job_titles=[
        "LLM 엔지니어",
        "MLOps 엔지니어",
        "머신러닝 엔지니어",
        "딥러닝 연구원",
        "컴퓨터비전 엔지니어",
        "NLP 엔지니어",
        "AI 플랫폼 개발자",
        "데이터사이언티스트 (ML)",
        "생성형 AI 엔지니어",
    ],
    skills_hint=[
        "Python", "PyTorch", "TensorFlow", "LangChain", "LlamaIndex",
        "OpenAI API", "Anthropic API", "RAG", "Vector DB",
        "Hugging Face", "CUDA", "MLflow", "Kubeflow", "Airflow",
        "Docker", "Kubernetes", "AWS SageMaker", "fine-tuning",
        "RLHF", "Prompt Engineering", "Faiss", "Pinecone",
    ],
)


if __name__ == "__main__":
    args = parse_args("AI/ML 더미 채용공고 생성")
    run_category_generator(CONFIG, count=args.count, use_cache=args.use_cache)
