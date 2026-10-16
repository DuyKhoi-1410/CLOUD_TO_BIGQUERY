# -*- coding: utf-8 -*-
"""
pipeline.py
===========
Apache Beam NLP Pipeline - Xử lý dữ liệu YouTube comments.
Đọc dữ liệu từ GCS, xử lý NLP (làm sạch, NER, chuẩn hóa tên,
phân tích cảm xúc) và ghi kết quả ra file JSONL cục bộ.
"""

import json
import re
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import emoji
from flashtext import KeywordProcessor
from google.cloud import storage
from transformers import pipeline as hf_pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F

logger = logging.getLogger(__name__)


# ==========================================
# 1. CÁC HÀM XỬ LÝ NLP & LÀM SẠCH CỐT LÕI
# ==========================================

class CleanTextFn(beam.DoFn):
    """Làm sạch văn bản bình luận."""

    def process(self, record):
        text = record.get('raw_text', '')
        if not text:
            yield record
            return
        text = emoji.demojize(text)
        text = re.sub(r'http[s]?://\S+', ' ', text)
        text = re.sub(r'[^\w\s\.,!?@_:-]', ' ', text)
        text = re.sub(r'(.)\1{2,}', r'\1', text)
        text = re.sub(r'\s+', ' ', text).strip()
        record['cleaned_text'] = text
        yield record


class ExtractPersonNERFn(beam.DoFn):
    """Trích xuất thực thể tên người (NER) bằng mô hình NlpHUST."""

    def setup(self):
        self.device = 0 if torch.cuda.is_available() else -1
        self.ner_pipeline = hf_pipeline(
            "ner",
            model="NlpHUST/ner-vietnamese-electra-base",
            aggregation_strategy="simple",
            device=self.device
        )

    def process(self, record):
        text = record.get('cleaned_text', '')
        if text:
            try:
                entities = self.ner_pipeline(text)
                persons = [ent['word'] for ent in entities if 'PER' in ent['entity_group']]
                record['extracted_persons'] = list(set(persons))
                record['has_person'] = len(persons) > 0
            except Exception as e:
                logger.warning("NER failed: %s", e)
                record['extracted_persons'] = []
                record['has_person'] = False
        else:
            record['extracted_persons'] = []
            record['has_person'] = False
        yield record


class NormalizeEntityFn(beam.DoFn):
    """Chuẩn hóa tên nghệ sĩ, người nổi tiếng và bí danh."""

    def setup(self):
        self.keyword_processor = KeywordProcessor(case_sensitive=False)
        alias_dict = {
            # ==========================================
            # 1. NHÓM 2 NGÀY 1 ĐÊM & GAMESHOW ĂN KHÁCH
            # ==========================================
            "Trường Giang": [
                "trường giang", "truong giang", "mười khó", "muoi kho", "tg", "anh giang", "giang ca"
            ],
            "Hiếu thứ 2": [
                "hieuthuhai", "hieu thu hai", "hiếu thứ hai", "hiếu thứ 2", "hieuthu2", "hth",
                "atsh hieuthuhai", "chủ tịch hieu"
            ],
            "Lê Dương Bảo Lâm": [
                "lê dương bảo lâm", "le duong bao lam", "dương lâm", "duong lam",
                "lee min ho đồng nai", "lee min ho dong nai", "ldbl", "lê dương", "le duong",
                "dương lâm đồng nai"
            ],
            "Kiều Minh Tuấn": [
                "kiều minh tuấn", "kieu minh tuan", "kmt", "anh tuấn", "anh tuan", "tuấn sạp"
            ],
            "Ngô Kiến Huy": [
                "ngô kiến huy", "ngo kien huy", "nkh", "chàng bắp", "chang bap", "bắp", "bap"
            ],
            "Cris Phan": [
                "cris phan", "crisphan", "cris", "cris devil gamer", "phan vũ thanh cris"
            ],
            "Song Luân": [
                "song luân", "song luan", "songluan"
            ],
            "Cody Nam Võ": [
                "cody nam võ", "cody nam vo", "cody", "nam võ", "nam vo", "cody namvo"
            ],
            "Võ Tấn Phát": [
                "võ tấn phát", "vo tan phat", "vtp", "tấn phát", "tan phat"
            ],

            # ==========================================
            # 2. ANH TRAI SAY HI & RAP VIỆT
            # ==========================================
            "Quang Hùng MasterD": [
                "quang hùng masterd", "quang hung masterd", "quanghungmasterd",
                "quang hùng", "quang hung", "qhm", "masterd", "bé lười"
            ],
            "CONGB": [
                "congb", "công b", "cong b", "côngb", "congb official"
            ],
            "JSOL": [
                "jsol", "j sol", "j-sol", "thái sơn"
            ],
            "Wren Evans": [
                "wren evans", "wren evan", "wren", "wrenevans", "lê phan"
            ],
            "Đặng Hồng Hải": [
                "đặng hồng hải", "dang hong hai", "đặng hải", "dang hai", "đhh", "dhh",
                "hồng hải", "hong hai"
            ],
            "Dương Domic": [
                "dương domic", "duong domic", "duongdomic", "d domic", "trần đăng dương"
            ],
            "HURRYKNG": [
                "hurrykng", "hurry kng", "hurrykngg", "khang", "phạm bảo khang"
            ],
            "WEAN": [
                "wean", "wean le", "wean lê", "weanle", "Lê Thế Anh"
            ],
            "Sơn.K": [
                "sơn.k", "son.k", "sơn k", "son k", "sơnk", "sonk"
            ],
            "Xuân Định K.Y": [
                "xuân định k.y", "xuan dinh k.y", "xuân định ky", "xuan dinh ky",
                "xuân định", "xuan dinh", "xdk"
            ],
            "Pháp Kiều": [
                "pháp kiều", "phap kieu", "phapkieu", "nguyễn pháp kiều"
            ],
            "CoolKid": [
                "coolkid", "cool kid", "cool-kid"
            ],
            "Bùi Trường Linh": [
                "bùi trường linh", "bui truong linh", "buitruonglinh", "btl"
            ],
            "Captain Boy": [
                "captain boy", "captainboy", "captain", "cap boy", "hoàng thế nguyên"
            ],
            "Jaysonlei": [
                "jaysonlei", "jayson lei"
            ],
            "Saabirose": [
                "saabirose", "saabi rose"
            ],
            "HYO": [
                "hyo", "hyo official"
            ],
            "Vương Bình": [
                "vương bình", "vuong binh"
            ],
            "Hùng Huỳnh": [
                "hùng huỳnh", "hung huynh", "hunghuynh"
            ],
            "Rhyder": [
                "rhyder", "quang anh", "nguyễn quang anh"
            ],
            "Negav": [
                "negav", "đặng thành an"
            ],
            "Ali Hoàng Dương": [
                "ali hoàng dương", "ali hoang duong"
            ],
            "Isaac": [
                "isaac", "isaac 365", "phạm lưu tuấn tài"
            ],

            # ==========================================
            # 3. CA SĨ, NGHỆ SĨ & RAPPER NỔI TIẾNG KHÁC
            # ==========================================
            "Phương Mỹ Chi": [
                "phương mỹ chi", "phuong my chi", "pmc", "cô bé dân ca"
            ],
            "Đức Phúc": [
                "đức phúc", "duc phuc"
            ],
            "Bích Phương": [
                "bích phương", "bich phuong"
            ],
            "Trấn Thành": [
                "trấn thành", "tran thanh", "a xìn", "a xin", "mc tran thanh", "huỳnh trấn thành"
            ],
            "Pháo": [
                "pháo", "phao", "pháo northside", "nguyễn diệu huyền"
            ],
            "Hữu Đằng": [
                "hữu đằng", "huu dang"
            ],
            "Ánh Sáng": [
                "ánh sáng", "anh sang"
            ],
            "Yeolan": [
                "yeolan", "yeo lan"
            ],
            "Bùi Duy Ngọc": [
                "bùi duy ngọc", "bui duy ngoc"
            ],
            "Hari Won": [
                "hari won", "hariwon"
            ],
            "Sơn Tùng M-TP": [
                "sơn tùng", "son tung", "m-tp", "sơn tùng m-tp", "thanh đồng", "nguyễn thanh tùng"
            ],
            "Jack": [
                "jack", "trịnh phương tuấn", "j97"
            ],
            "Mỹ Tâm": [
                "mỹ tâm", "my tam", "họa mi tóc nâu"
            ],
            "Hồ Ngọc Hà": [
                "hồ ngọc hà", "ho ngoc ha"
            ],
            "Đen Vâu": [
                "đen vâu", "den vau", "đen", "nguyễn đức cường"
            ],
            "Binz": [
                "binz", "bigcityboi", "lê nguyễn trung đan"
            ],
            "Suboi": [
                "suboi", "hàng lâm ánh ngọc"
            ],
            "Karik": [
                "karik", "phạm hoàng khoa"
            ],
            "Wowy": [
                "wowy", "nguyễn văn huy"
            ],
            "JustaTee": [
                "justatee", "thanh tuấn", "nguyễn thanh tuấn"
            ],
            "B Ray": [
                "b ray", "bray", "trần thiện thanh bảo"
            ],
            "Rhymastic": [
                "rhymastic", "vũ đức thiện"
            ],
            "Tóc Tiên": [
                "tóc tiên", "toc tien", "nguyễn khoa tóc tiên"
            ],
            "Hoàng Thùy Linh": [
                "hoàng thùy linh", "hoang thuy linh"
            ],
            "Chi Pu": [
                "chi pu", "nguyễn thuỳ chi"
            ],
            "Soobin Hoàng Sơn": [
                "soobin", "soobin hoàng sơn", "nguyễn hoàng sơn"
            ],
            "Erik": [
                "erik", "lê trung thành"
            ],
            "Hòa Minzy": [
                "hòa minzy", "hoa minzy", "nguyễn thị hòa"
            ],
            "Amee": [
                "amee", "trần huyền mỹ"
            ],
            "Monstera": [
                "monstera"
            ],

            # ==========================================
            # 4. NHÂN VẬT TRONG NỘI DUNG FOREST STUDIO / SỰ KIỆN KHÁC
            # ==========================================
            "Hai Thành": [
                "hai thành", "hai thanh"
            ],
            "Tám Cúc": [
                "tám cúc", "tam cuc"
            ],
            "Sáu Tút": [
                "sáu tút", "sau tut"
            ],
            "Tư Ngọc": [
                "tư ngọc", "tu ngoc"
            ],
        }
        self.keyword_processor.add_keywords_from_dict(alias_dict)

    def process(self, record):
        persons = record.get("extracted_persons", []) or []
        text = record.get("cleaned_text", "") or ""
        title = record.get("video_title", "") or ""

        normalized = set()

        # Bước 1: Chuẩn hóa các tên NER đã phát hiện
        for person in persons:
            person = str(person).strip()
            if not person:
                continue
            matches = self.keyword_processor.extract_keywords(person)
            if matches:
                normalized.update(matches)
            # Không tự động thêm tên NER lạ vào kết quả.
            # Chỉ giữ tên nếu khớp từ điển bí danh để giảm nhận nhầm tên bài hát/chủ đề.

        # Bước 2: Quét trực tiếp nội dung bình luận để bổ sung tên đã biết
        text_matches_with_spans = self.keyword_processor.extract_keywords(text, span_info=True)
        normalized.update(match[0] for match in text_matches_with_spans)
        record["person_mentions"] = [
            {"person": match[0], "start": match[1], "end": match[2]}
            for match in text_matches_with_spans
        ]

        # Bước 3: Xử lý bí danh ngắn theo ngữ cảnh video
        title_lower = title.lower()
        text_lower = text.lower()

        if "hieuthuhai" in title_lower or "hiếu thứ hai" in title_lower or "hieu thu hai" in title_lower:
            if any(alias in text_lower for alias in ["hiếu", "hieu", "thằng hiếu", "thang hieu"]):
                normalized.add("Hiếu thứ 2")

        if "2n1d" in title_lower or "2 ngày 1 đêm" in title_lower:
            initials = {
                "kmt": "Kiều Minh Tuấn",
                "nkh": "Ngô Kiến Huy",
                "vtp": "Võ Tấn Phát",
                "ldbl": "Lê Dương Bảo Lâm",
            }
            for short_name, full_name in initials.items():
                if short_name in text_lower.split():
                    normalized.add(full_name)

        # Bí danh "Hiếu" chỉ được gán cho HIEUTHUHAI khi tiêu đề có nhắc rõ nghệ sĩ này.
        if (
            any(k in title_lower for k in ["hieuthuhai", "hiếu thứ hai", "hieu thu hai"])
            and re.search(r"\bhiếu\b|\bhieu\b", text_lower)
        ):
            normalized.add("Hiếu thứ 2")

        record["normalized_persons"] = sorted(list(normalized), key=str.casefold)
        # has_person được quyết định bởi tên khớp từ điển, không lấy nguyên kết quả NER.
        record["has_person"] = len(normalized) > 0
        yield record


class SentimentAnalysisFn(beam.DoFn):
    """Phân tích cảm xúc bằng mô hình PhoBERT."""

    def setup(self):
        model_name = "wonrax/phobert-base-vietnamese-sentiment"
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name).to(self.device)
        self.model.eval()

    def process(self, record):
        text = record.get('cleaned_text', '')
        if text:
            try:
                inputs = self.tokenizer(
                    text, return_tensors="pt", truncation=True, max_length=256
                ).to(self.device)
                with torch.no_grad():
                    outputs = self.model(**inputs)
                probs = F.softmax(outputs.logits, dim=1).squeeze().tolist()
                record['sentiment_neg_prob'] = round(probs[0], 4)
                record['sentiment_pos_prob'] = round(probs[1], 4)
                record['sentiment_neu_prob'] = round(probs[2], 4)
                labels = ['NEGATIVE', 'POSITIVE', 'NEUTRAL']
                record['sentiment_label'] = labels[probs.index(max(probs))]
            except Exception as e:
                logger.warning("Sentiment failed: %s", e)
                record['sentiment_label'] = 'ERROR'
        else:
            record['sentiment_label'] = 'EMPTY'

        # Gộp danh sách nghệ sĩ chuẩn hóa thành chuỗi phân tách bằng dấu phẩy
        record['normalized_persons_str'] = ", ".join(record.get('normalized_persons', []))

        yield {
            "comment_id": record.get("comment_id"),
            "video_id": record.get("video_id"),
            "channel_title": record.get("channel_title"),
            "video_title": record.get("video_title"),
            "program_name": record.get("program_name", "Chưa xác định"),
            "raw_text": record.get("raw_text"),
            "cleaned_text": record.get("cleaned_text"),
            "has_person": record.get("has_person"),
            "normalized_persons": record.get("normalized_persons_str"),
            "sentiment_label": record.get("sentiment_label"),
            "sentiment_pos_prob": record.get("sentiment_pos_prob"),
            "sentiment_neg_prob": record.get("sentiment_neg_prob"),
            "sentiment_neu_prob": record.get("sentiment_neu_prob"),
            "comment_published_at": record.get("comment_published_at"),
        }


# ==========================================
# Ô hỗ trợ nâng cao – Nhận diện chương trình
# ==========================================

def extract_program_name(video_title, channel_title=None, existing_program=None):
    """Xác định tên chương trình/show từ tiêu đề video và kênh."""
    title = (str(video_title) or "").lower()
    channel = (str(channel_title) or "").lower()

    # 1. Ưu tiên 1: ĐÔNG TÂY PROMOTION
    if any(kw in channel for kw in ["đông tây", "dong tay"]):
        if any(kw in title for kw in ["2n1d", "2 ngày 1 đêm"]): return "2 Ngày 1 Đêm"
        if "hành trình rực rỡ" in title: return "Hành Trình Rực Rỡ"
        if "tổ đội" in title: return "Tổ Đội 1 Không 2"
        if "la cà thú vị" in title: return "La Cà Thú Vị"
        if "7 nụ" in title or "bảy nụ" in title: return "7 Nụ Cười Xuân"
        if "nhanh như chớp" in title: return "Nhanh Như Chớp"
        if "kỳ tài thách đấu" in title: return "Kỳ Tài Thách Đấu"
        if "BTS ATVCDN" in title: return "Anh Trai Và Cái Đuôi Nhỏ"
        if "Anh Trai Và Cái Đuôi Nhỏ" in title: return "Anh Trai Và Cái Đuôi Nhỏ"

    # 2. Ưu tiên 2: FOREST STUDIO
    elif "forest" in channel:
        if any(kw in title for kw in ["xuân hạ thu đông", "xhtdrlx", "xhtđrlx"]): return "Xuân Hạ Thu Đông Rồi Lại Xuân"
        if any(kw in title for kw in ["running man", "RUN MOMENT", "BTS", "chơi là chạy"]): return "Running Man Vietnam"
        if "mùi vị những chuyến đi" in title: return "Mùi Vị Những Chuyến Đi"
        if "eye contact live" in title: return "Eye Contact Live"

    # 3. Ưu tiên 3: VIE CHANNEL
    elif "vie" in channel:
        if any(kw in title for kw in ["say hi", "anh trai", "say real", "anh trai say hi"]):
            return "Anh Trai Say Hi"
        if any(kw in title for kw in ["rap việt", "rap viet"]): return "Rap Việt"
        if any(kw in title for kw in ["The Masked Singer", "ca sĩ mặt nạ"]): return "Ca Sĩ Mặt Nạ"
        if any(kw in title for kw in ["người ấy là ai", "nala"]): return "Người Ấy Là Ai"
        if "siêu trí tuệ" in title: return "Siêu Trí Tuệ Việt Nam"
        if any(kw in title for kw in ["THSH", "Tinh Hà Say Hi", "TINH HÀ SAY HI", "UNCUT"]):
            return "Tinh Hà Say Hi"

    # 4. Ưu tiên 4: YEAH1
    elif "yeah1" in channel:
        if any(kw in title for kw in ["anh trai vượt ngàn chông gai", "atvncg", "chông gai"]):
            return "Anh Trai Vượt Ngàn Chông Gai"
        if any(kw in title for kw in ["chị đẹp đạp gió", "chị đẹp rẽ sóng", "chị đẹp"]):
            return "Chị Đẹp Đạp Gió Rẽ Sóng"
        if "gia đình haha" in title: return "Gia Đình Haha"
        if "Qua Nhà Bật Nhạc" in title: return "Qua Nhà Bật Nhạc"

    # ==========================================
    # 5. LƯỚI BẮT DỰ PHÒNG (FALLBACK TOÀN CỤC)
    # Dành cho trường hợp mất channel_title hoặc thuộc kênh cắt clip bên thứ 3
    # ==========================================
    if any(kw in title for kw in ["2n1d", "2 ngày 1 đêm"]): return "2 Ngày 1 Đêm"
    if any(kw in title for kw in ["say hi", "say real", "tinh hà", "anh trai say hi"]): return "Anh Trai Say Hi"
    if any(kw in title for kw in ["rap việt", "rap viet"]): return "Rap Việt"
    if any(kw in title for kw in ["the masked singer", "ca sĩ mặt nạ"]): return "Ca Sĩ Mặt Nạ"
    if any(kw in title for kw in ["chông gai", "atvncg", "anh trai vượt ngàn"]): return "Anh Trai Vượt Ngàn Chông Gai"
    if any(kw in title for kw in ["chị đẹp đạp gió", "chị đẹp"]): return "Chị Đẹp Đạp Gió Rẽ Sóng"
    if "running man" in title: return "Running Man Vietnam"

    # 6. Cuối cùng, nếu không khớp nhưng có dữ liệu từ trước
    if existing_program and str(existing_program).strip().lower() not in ("none", "null", "", "nan"):
        return str(existing_program).strip()

    return "Chưa xác định"


class AddProgramNameFn(beam.DoFn):
    def process(self, record):
        record["program_name"] = extract_program_name(
            video_title=record.get("video_title", ""),
            channel_title=record.get("channel_title", ""),
            existing_program=record.get("program_name"),
        )
        yield record


class ExpandPersonRowsFn(beam.DoFn):
    """
    Tạo một dòng cho mỗi nhân vật.
    Ưu tiên phân tích cảm xúc theo câu/mệnh đề chứa nhân vật,
    thay vì lấy cửa sổ 100 ký tự xung quanh tên.
    """

    def process(self, record):
        text = record.get("cleaned_text", "") or ""
        persons = record.get("normalized_persons", []) or []
        mentions = record.get("person_mentions", []) or []

        if not text or not persons:
            return

        # Các từ nối thường thể hiện sự đối lập giữa các ý.
        # Giữ lại dấu phân cách để không làm mất nội dung.
        split_pattern = (
            r"(?<=[.!?;])\s+"
            r"|\s+(?i:nhưng|trong khi|còn|"
            r"tuy nhiên|ngược lại|whereas|but)\s+"
        )

        # Chia văn bản thành các mệnh đề và giữ vị trí
        # bắt đầu/kết thúc trong văn bản gốc.
        clauses = []
        last_end = 0

        for match in re.finditer(split_pattern, text):
            start, end = last_end, match.start()
            clause = text[start:end].strip()

            if clause:
                actual_start = text.find(clause, start, end)
                clauses.append((
                    actual_start,
                    actual_start + len(clause),
                    clause
                ))

            last_end = match.end()

        tail = text[last_end:].strip()
        if tail:
            actual_start = text.find(tail, last_end)
            clauses.append((
                actual_start,
                actual_start + len(tail),
                tail
            ))

        # Nếu không tách được câu, giữ nguyên bình luận.
        if not clauses:
            clauses = [(0, len(text), text)]

        # Xác định vị trí nhắc đến từng nhân vật.
        person_contexts = {}

        for person in sorted(set(persons), key=str.casefold):
            person_mentions_list = [
                m for m in mentions
                if m.get("person") == person
            ]

            contexts = []

            for mention in person_mentions_list:
                mention_start = mention.get("start", -1)
                mention_end = mention.get("end", -1)

                for start, end, clause in clauses:
                    if start <= mention_start and mention_end <= end:
                        contexts.append(clause)
                        break

            # Loại bỏ mệnh đề trùng lặp.
            contexts = list(dict.fromkeys(contexts))

            # Không tìm thấy vị trí khớp thì dùng toàn bộ bình luận
            # làm phương án dự phòng, đồng thời cần kiểm tra thủ công.
            if not contexts:
                contexts = [text]

            person_contexts[person] = contexts

        # Xuất một dòng cho mỗi nhân vật.
        for person, contexts in person_contexts.items():
            for context in contexts:
                row = dict(record)
                row["target_person"] = person
                row["normalized_persons"] = [person]
                row["sentiment_context"] = context

                # SentimentAnalysisFn sẽ đọc trường này.
                row["cleaned_text"] = context

                yield row


# ==========================================
# Các hàm hỗ trợ Join và đọc dữ liệu
# ==========================================

def parse_json(element):
    return json.loads(element)


def extract_kv(record):
    return (record['video_id'], record)


def join_video_and_comments(element):
    video_id, grouped_data = element
    videos = grouped_data['videos']
    comments = grouped_data['comments']
    if not comments:
        return
    video_info = videos[0] if videos else {}
    channel_title = video_info.get('channel_title', 'Unknown')
    video_title = video_info.get('video_title', 'Unknown')
    for comment in comments:
        comment['channel_title'] = channel_title
        comment['video_title'] = video_title
        yield comment


# ==========================================
# Tự động tìm đường dẫn GCS lần cào mới nhất
# ==========================================

def get_latest_run_paths(bucket_name="bd-raw-data-bronze"):
    """Hàm tự động tìm chính xác đường dẫn của lần cào mới nhất trên GCS."""
    client = storage.Client()
    blobs = list(client.list_blobs(bucket_name, prefix="bronze/youtube/"))

    run_timestamps = set()
    for blob in blobs:
        if "run_ts=" in blob.name:
            ts_part = [p for p in blob.name.split('/') if p.startswith('run_ts=')]
            if ts_part:
                run_timestamps.add(ts_part[0])

    if not run_timestamps:
        raise RuntimeError("Không tìm thấy dữ liệu cào nào trên GCS!")

    latest_run_ts = sorted(list(run_timestamps))[-1]

    sample_blob = [b for b in blobs if latest_run_ts in b.name][0]
    date_part = [p for p in sample_blob.name.split('/') if p.startswith('run_date=')][0]

    video_path = f"gs://{bucket_name}/bronze/youtube/{date_part}/{latest_run_ts}/video_snapshots/*.jsonl"
    comment_path = f"gs://{bucket_name}/bronze/youtube/{date_part}/{latest_run_ts}/comments/*.jsonl"

    logger.info("Hệ thống tự động phát hiện phiên bản cào mới nhất: %s", latest_run_ts)
    return video_path, comment_path


# ==========================================
# Hàm chính chạy Apache Beam Pipeline
# ==========================================

def run(bucket_name: str = "bd-raw-data-bronze"):
    """Chạy toàn bộ Beam pipeline xử lý NLP và ghi kết quả JSONL."""
    video_pattern, comment_pattern = get_latest_run_paths(bucket_name)

    options = PipelineOptions(flags=[])

    with beam.Pipeline(options=options) as p:
        videos = (
            p | "Read Latest Videos" >> beam.io.ReadFromText(video_pattern)
              | "Parse Video JSON" >> beam.Map(parse_json)
              | "Key Video by ID" >> beam.Map(extract_kv)
        )

        comments = (
            p | "Read Latest Comments" >> beam.io.ReadFromText(comment_pattern)
              | "Parse Comment JSON" >> beam.Map(parse_json)
              | "Key Comment by ID" >> beam.Map(extract_kv)
        )

        joined = (
            {'videos': videos, 'comments': comments}
            | "CoGroupByKey" >> beam.CoGroupByKey()
            | "Merge Video Info" >> beam.FlatMap(join_video_and_comments)
            | "Clean Text" >> beam.ParDo(CleanTextFn())
            | "Extract Program" >> beam.ParDo(AddProgramNameFn())
        )

        # Lọc bỏ các bình luận trùng lặp dựa trên comment_id
        def filter_unique_comments(element):
            return (element['comment_id'], element)

        unique_comments = (
            joined
            | "Map to Key comment_id" >> beam.Map(filter_unique_comments)
            | "Group by comment_id" >> beam.GroupByKey()
            | "Extract First Record per comment_id" >> beam.Map(lambda kv: kv[1][0])
        )

        # Bảng 1: Phân tích tất cả bình luận (lấy từ dữ liệu đã lọc sạch trùng)
        all_comments = (
            unique_comments
            | "Normalize Names (All)" >> beam.ParDo(NormalizeEntityFn())
            | "Sentiment (All)" >> beam.ParDo(SentimentAnalysisFn())
            | "Convert All to JSON" >> beam.Map(lambda x: json.dumps(x, ensure_ascii=False))
        )

        all_comments | "Write All Comments" >> beam.io.WriteToText(
            '/tmp/all_comments_sentiment', file_name_suffix='.jsonl', shard_name_template=''
        )

        # Bảng 2: Phân tích theo nhân vật (lấy từ dữ liệu đã lọc sạch trùng)
        person_rows = (
            unique_comments
            | "Extract NER" >> beam.ParDo(ExtractPersonNERFn())
            | "Normalize Names (Person)" >> beam.ParDo(NormalizeEntityFn())
            | "Filter Has Person" >> beam.Filter(lambda x: x.get('has_person', False))
            | "Expand Persons" >> beam.ParDo(ExpandPersonRowsFn())
            | "Sentiment (Person)" >> beam.ParDo(SentimentAnalysisFn())
            | "Convert Person to JSON" >> beam.Map(lambda x: json.dumps(x, ensure_ascii=False))
        )

        person_rows | "Write Person Sentiment" >> beam.io.WriteToText(
            '/tmp/person_sentiment', file_name_suffix='.jsonl', shard_name_template=''
        )

    logger.info("Pipeline hoàn thành. Kết quả ghi tại /tmp/")
    return {
        "all_comments_path": "/tmp/all_comments_sentiment.jsonl",
        "person_sentiment_path": "/tmp/person_sentiment.jsonl",
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.info("Bắt đầu chạy Pipeline xử lý dữ liệu tự động...")
    result = run()
    logger.info("Hoàn thành! Kết quả: %s", result)
