# OCR_Receipts

El requerimiento constituye una arquitectura de microservicios utilizando contenedores. La API será desplegada usando Flask y aceptará los siguientes inputs: imagen JPG, archivo PDF, código QR (opcional) y el RUC de la empresa receptora.

Se debe extraer la data y devolver los siguientes datos en un diccionario:

* RUC del emisor
* Tipo de documento
* Serie y correlativo
* Fecha de emisión
* Fecha de vencimiento (opcional)
* Valor venta
* IGV
* ICBPER (si lo muestra)
* Otros cargos
* Importe total

**IMPORTANTE:** Se debe almacenar la foto localmente para trazabilidad, el reto está en construir expresiones regulares lo suficientemente robustas
