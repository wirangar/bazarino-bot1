import io
import os
import random
import logging
from PIL import Image, ImageDraw, ImageFont
from config import HAFEZ_QUOTES

log = logging.getLogger("bazarino")

async def generate_invoice(order_id, user_data, cart, total, discount):
    from PIL import Image, ImageDraw, ImageFont

    width, height = 700, 1000
    bg_color = (248, 249, 250)
    header_color = (40, 167, 69)
    text_color = (33, 37, 41)
    secondary_text_color = (108, 117, 125)
    border_color = (222, 226, 230)

    img = Image.new("RGB", (width, height), color=bg_color)

    # Add background pattern
    pattern_path = "assets/background_pattern.png"
    if os.path.exists(pattern_path):
        try:
            pattern = Image.open(pattern_path).convert("RGBA")
            # Create a transparent overlay
            overlay = Image.new('RGBA', img.size, (255, 255, 255, 0))
            draw_overlay = ImageDraw.Draw(overlay)
            # Tile the pattern
            for y in range(0, height, pattern.height):
                for x in range(0, width, pattern.width):
                    overlay.paste(pattern, (x, y), pattern)
            # Control opacity
            alpha = overlay.split()[3]
            alpha = Image.eval(alpha, lambda p: p // 4) # Reduce opacity
            img.paste(Image.composite(overlay, img, alpha))
        except Exception as e:
            log.error(f"Background pattern error: {e}")

    draw = ImageDraw.Draw(img)

    # Fonts
    font_dir = "fonts"
    try:
        title_font = ImageFont.truetype(os.path.join(font_dir, "Vazir.ttf"), 32)
        header_font = ImageFont.truetype(os.path.join(font_dir, "Vazir.ttf"), 22)
        body_font = ImageFont.truetype(os.path.join(font_dir, "Vazir.ttf"), 18)
        small_font = ImageFont.truetype(os.path.join(font_dir, "arial.ttf"), 14)
    except Exception as e:
        log.error(f"Font loading error: {e}, falling back to default fonts")
        title_font = header_font = body_font = small_font = ImageFont.load_default()

    # Header
    draw.rectangle([(0, 0), (width, 100)], fill=header_color)
    logo_path = "logo.png"
    if os.path.exists(logo_path):
        try:
            logo = Image.open(logo_path).resize((80, 80))
            img.paste(logo, (20, 10), mask=logo)
        except Exception as e:
            log.error(f"Logo loading error: {e}")
    draw.text((width - 40, 50), "فاکتور فروش", fill=(255, 255, 255), font=title_font, anchor="ra")
    draw.text((width - 40, 85), "Bazarino Invoice", fill=(220, 220, 220), font=small_font, anchor="ra")

    # Order Info
    y = 140
    margin = 40
    draw.text((width - margin, y), f"شماره سفارش: {order_id}", font=header_font, fill=text_color, anchor="ra")
    draw.text((margin, y), f"Order ID: #{order_id}", font=small_font, fill=secondary_text_color, anchor="la")
    y += 50

    # Customer Info
    draw.rounded_rectangle([(margin, y), (width - margin, y + 100)], radius=10, fill=(255, 255, 255), outline=border_color)

    info_y = y + 20
    draw.text((width - margin - 20, info_y), f"نام مشتری: {user_data.get('name', 'N/A')}", font=body_font, fill=text_color, anchor="ra")
    info_y += 30
    draw.text((width - margin - 20, info_y), f"مقصد: {user_data.get('dest', 'N/A')}", font=body_font, fill=text_color, anchor="ra")
    info_y = y + 20
    draw.text((margin + 20, info_y), f"Address: {user_data.get('address', 'N/A')}", font=small_font, fill=secondary_text_color, anchor="la")
    info_y += 20
    draw.text((margin + 20, info_y), f"Postal Code: {user_data.get('postal', 'N/A')}", font=small_font, fill=secondary_text_color, anchor="la")
    info_y += 20
    draw.text((margin + 20, info_y), f"Phone: {user_data.get('phone', 'N/A')}", font=small_font, fill=secondary_text_color, anchor="la")

    y += 130

    # Products Table
    draw.text((width - margin, y), "محصولات / Prodotti", font=header_font, fill=text_color, anchor="ra")
    y += 40

    draw.line([(margin, y), (width - margin, y)], fill=border_color, width=2)
    y += 15
    draw.text((width - margin - 20, y), "محصول", font=body_font, fill=secondary_text_color, anchor="ra")
    draw.text((width / 2 + 70, y), "تعداد", font=body_font, fill=secondary_text_color, anchor="ma")
    draw.text((width / 2 - 70, y), "قیمت واحد", font=body_font, fill=secondary_text_color, anchor="ma")
    draw.text((margin + 20, y), "جمع", font=body_font, fill=secondary_text_color, anchor="la")
    y += 15
    draw.line([(margin, y), (width - margin, y)], fill=border_color, width=1)

    for item in cart:
        y += 25
        subtotal = item['qty'] * item['price']
        draw.text((width - margin - 20, y), item['fa'], font=body_font, fill=text_color, anchor="ra")
        draw.text((width / 2 + 70, y), str(item['qty']), font=body_font, fill=text_color, anchor="ma")
        draw.text((width / 2 - 70, y), f"{item['price']:.2f}€", font=body_font, fill=text_color, anchor="ma")
        draw.text((margin + 20, y), f"{subtotal:.2f}€", font=body_font, fill=text_color, anchor="la")
        y += 25
        if item != cart[-1]:
            draw.line([(margin + 20, y), (width - margin - 20, y)], fill=border_color, width=1)

    # Totals
    y += 30
    draw.line([(width / 2, y), (width - margin, y)], fill=border_color, width=1)
    y += 15
    draw.text((width - margin, y), f"تخفیف: {discount:.2f}€", font=body_font, fill=text_color, anchor="ra")
    draw.text((width / 2 + 20, y), f"Sconto", font=small_font, fill=secondary_text_color, anchor="la")
    y += 30
    draw.text((width - margin, y), f"مبلغ نهایی: {total:.2f}€", font=header_font, fill=header_color, anchor="ra")
    draw.text((width / 2 + 20, y), f"Totale", font=body_font, fill=secondary_text_color, anchor="la")
    y += 60

    # Notes & Hafez
    if user_data.get('notes') and user_data['notes'].strip():
        draw.text((width - margin, y), "یادداشت شما:", font=body_font, fill=text_color, anchor="ra")
        y += 25
        draw.text((width - margin, y), user_data['notes'], font=small_font, fill=secondary_text_color, anchor="ra")
        y += 40

    if HAFEZ_QUOTES:
        hafez = random.choice(HAFEZ_QUOTES)
        draw.text((width / 2, y), "✨ فال حافظ ✨", font=header_font, fill=text_color, anchor="mm")
        y += 35
        draw.text((width / 2, y), hafez["fa"], font=body_font, fill=secondary_text_color, anchor="mm")
        y += 25
        draw.text((width / 2, y), hafez["it"], font=small_font, fill=secondary_text_color, anchor="mm")

    # Footer
    footer_y = height - 60
    draw.rectangle([(0, footer_y), (width, height)], fill=header_color)
    draw.text((width / 2, footer_y + 30), "بازارینو - طعم ایران در ایتالیا | Bazarino - Sapori d'Iran in Italia", fill=(255, 255, 255), font=body_font, anchor="mm")

    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer
