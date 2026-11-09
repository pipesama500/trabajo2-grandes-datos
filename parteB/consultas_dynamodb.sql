-- Trabajo 2, parte B - Consultas PartiQL en DynamoDB (región us-east-1)
-- DynamoDB -> PartiQL editor

-- Pregunta del proyecto: ¿cómo varió la radiación de un departamento minuto a minuto?
SELECT departamento, ventana_inicio, conteo_eventos, radiacion_promedio_w_m2
FROM "ventanas_radiacion"
WHERE "departamento" = 'LA_GUAJIRA';

-- Eventos que llegaron repetidos (y solo se contaron una vez)
SELECT id_evento, departamento, repeticiones
FROM "eventos_vistos"
WHERE repeticiones > 0;

-- Eventos inválidos con su motivo
SELECT id_evento, motivo, evento_original
FROM "eventos_invalidos";
