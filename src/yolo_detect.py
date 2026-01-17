from ultralytics import YOLO
import cv2
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ImageDetector:
    def __init__(self, model_name='yolov8n.pt'):
        self.model = YOLO(model_name)
        self.results_data = []
    
    def detect_objects(self, image_path):
        """Run YOLO detection on an image"""
        try:
            results = self.model(image_path)
            detections = []
            
            for result in results:
                boxes = result.boxes
                for box in boxes:
                    class_id = int(box.cls[0])
                    confidence = float(box.conf[0])
                    class_name = self.model.names[class_id]
                    
                    detections.append({
                        'class_name': class_name,
                        'confidence': confidence
                    })
            
            return detections
            
        except Exception as e:
            logger.error(f"Error detecting objects in {image_path}: {str(e)}")
            return []
    
    def categorize_image(self, detections):
        """Categorize image based on detected objects"""
        if not detections:
            return 'other'
        
        class_names = [d['class_name'] for d in detections]
        
        # Check for person and product
        has_person = 'person' in class_names
        has_product = any(item in class_names for item in ['bottle', 'cup', 'bowl', 'vase'])
        
        if has_person and has_product:
            return 'promotional'
        elif has_product and not has_person:
            return 'product_display'
        elif has_person and not has_product:
            return 'lifestyle'
        else:
            return 'other'
    
    def process_images(self, image_dir='data/raw/images'):
        """Process all images in directory"""
        image_paths = list(Path(image_dir).rglob('*.jpg'))
        
        for image_path in image_paths:
            logger.info(f"Processing {image_path}")
            
            # Extract metadata from path
            parts = image_path.parts
            channel_name = parts[-2]
            message_id = image_path.stem
            
            # Detect objects
            detections = self.detect_objects(str(image_path))
            category = self.categorize_image(detections)
            
            # Get highest confidence detection
            if detections:
                top_detection = max(detections, key=lambda x: x['confidence'])
                detected_class = top_detection['class_name']
                confidence = top_detection['confidence']
            else:
                detected_class = None
                confidence = 0.0
            
            self.results_data.append({
                'message_id': int(message_id),
                'channel_name': channel_name,
                'image_path': str(image_path),
                'detected_class': detected_class,
                'confidence_score': confidence,
                'image_category': category,
                'all_detections': str(detections)
            })
        
        # Save results
        df = pd.DataFrame(self.results_data)
        output_path = 'data/processed/yolo_detections.csv'
        Path('data/processed').mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        
        logger.info(f"Saved detection results to {output_path}")
        return df

if __name__ == '__main__':
    detector = ImageDetector()
    results = detector.process_images()
    print(results.head())
    print(f"\nCategory distribution:\n{results['image_category'].value_counts()}")