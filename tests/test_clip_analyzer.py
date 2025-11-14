from modules.image_analyzer import ImageAnalyzer
from pathlib import Path
from tqdm import tqdm

def main():
    analyzer = ImageAnalyzer(reference_dir="reference", threshold=0.75)

    test_dir = Path("tests/images")
    images = list(test_dir.glob("*.*"))

    if not images:
        print("❌ Нет тестовых изображений в tests/images/")
        return

    print(f"\n🔍 Проверяем {len(images)} изображений...\n")
    results = []
    for img in tqdm(images):
        score = analyzer.compare(str(img))
        results.append((img.name, score))

    results.sort(key=lambda x: x[1], reverse=True)
    print("\n📊 Результаты по убыванию сходства:")
    for name, score in results:
        match = "✅" if score >= analyzer.threshold else "❌"
        print(f"{match} {name:30} — similarity={score:.3f}")

if __name__ == "__main__":
    main()
