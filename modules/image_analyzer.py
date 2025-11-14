import torch
import open_clip
from PIL import Image
from pathlib import Path
import numpy as np
import logging

logger = logging.getLogger(__name__)


class ImageAnalyzer:
    """
    Анализатор изображений на базе OpenAI CLIP (ViT-B/32).
    Используется для нахождения визуальных совпадений с эталонными изображениями.
    """

    def __init__(self, reference_dir="reference", model_name="ViT-B-32", threshold=0.75):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model_name = model_name
        self.threshold = threshold

        logger.info(f"Загружается модель CLIP ({model_name}) на {self.device}...")
        self.model, _, self.preprocess = open_clip.create_model_and_transforms(
            model_name, pretrained="openai"
        )
        self.model = self.model.to(self.device)
        self.references = []
        self._load_references(reference_dir)
        logger.info(f"Загружено эталонов: {len(self.references)}")

    def _open_image_safe(self, path: Path):
        """Безопасное открытие PNG/WEBP, включая прозрачные фоны."""
        img = Image.open(path)
        if img.mode in ("RGBA", "LA"):
            bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
            img = Image.alpha_composite(bg, img.convert("RGBA")).convert("RGB")
        else:
            img = img.convert("RGB")
        return img

    def _load_references(self, reference_dir):
        ref_dir = Path(reference_dir)
        for path in ref_dir.glob("*.*"):
            try:
                img = self._open_image_safe(path)
                image_tensor = self.preprocess(img).unsqueeze(0).to(self.device)
                with torch.no_grad():
                    emb = self.model.encode_image(image_tensor)
                    emb /= emb.norm(dim=-1, keepdim=True)
                self.references.append((path.name, emb))
            except Exception as e:
                logger.warning(f"Ошибка при загрузке эталона {path.name}: {e}")

    def compare(self, candidate_path: str) -> float:
        """Возвращает максимальное сходство (0–1) изображения с эталонами."""
        try:
            img = self._open_image_safe(Path(candidate_path))
            tensor = self.preprocess(img).unsqueeze(0).to(self.device)
            with torch.no_grad():
                emb = self.model.encode_image(tensor)
                emb /= emb.norm(dim=-1, keepdim=True)
            sims = [torch.cosine_similarity(ref_emb, emb).item() for _, ref_emb in self.references]
            return float(np.max(sims))
        except Exception as e:
            logger.error(f"Ошибка при сравнении {candidate_path}: {e}")
            return 0.0

    def is_match(self, candidate_path: str) -> bool:
        """True, если сходство выше порога."""
        return self.compare(candidate_path) >= self.threshold
