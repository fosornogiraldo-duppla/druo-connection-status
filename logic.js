// Logic for Duppla Dashboards

window.clasificarGrupoMeta = function (cliente) {
    const saldoMora = Number(cliente.saldo_en_mora || 0);
    const score = Number(cliente.credit_score_actual || 0);
    const deltaScore = Number(cliente.delta_credit_score || 0);
    const equity = Number(cliente.equity_actual_pct || 0);
    const proy5a = Number(cliente.equity_proyectada_5y_pct || 0);
    const moratoriaProm = Number(cliente.moratoria_promedio_pct || 0);
    const antiguedad = Number(cliente.antiguedad_meses || 0);

    // 0) Nuevos (temporal): menores a 12 meses
    if (antiguedad < 12) return 'Nuevos';

    // A) En riesgo (prioridad máxima)
    if (antiguedad >= 12) {
        if (saldoMora > 0) return 'En riesgo';
        if (moratoriaProm > 0) return 'En riesgo';
        if (deltaScore < 0 && score < 600) return 'En riesgo';
        if (proy5a < 30) return 'En riesgo';
    }

    // B) Off-Ramp (debe cumplir TODO)
    if (saldoMora === 0 &&
        equity >= 25 &&
        proy5a > 30 &&
        deltaScore > 0 &&
        score > 700) {
        return 'Off-Ramp';
    }

    // C) Construyendo (default intermedio)
    return 'Construyendo';
}

window.esListoBanco = function (cliente) {
    return cliente.equity_actual_pct >= 30 &&
        cliente.credit_score_actual >= 700 &&
        (cliente.dti_actual_pct === null || cliente.dti_actual_pct <= 10) &&
        cliente.saldo_en_mora === 0;
}

window.clasificarSegmentacionValeria = function (cliente) {
    const { antiguedad_meses, participacion_adquirida_pct, equity_actual_pct, habito_de_pago, moratoria_promedio_pct } = cliente;
    const isBuenHabito = habito_de_pago === 'Anticipado' || habito_de_pago === 'A tiempo';
    const grupoMeta = window.clasificarGrupoMeta(cliente);

    // Regla de consistencia: si está en riesgo, no puede salir en segmento "sano".
    if (grupoMeta === 'En riesgo') {
        return 'Inestables o en riesgo';
    }

    // 1. Avanzados Confiables
    if (equity_actual_pct >= 28 &&
        isBuenHabito &&
        moratoria_promedio_pct <= 5 &&
        antiguedad_meses >= 5) {
        return 'Avanzados confiables';
    }

    // 2. Estables en Construcción
    if (equity_actual_pct >= 20 &&
        moratoria_promedio_pct <= 15 &&
        antiguedad_meses >= 3) {
        return 'Estables en construcción';
    }

    // 3. Estancados con Potencial
    if (participacion_adquirida_pct <= 3 &&
        moratoria_promedio_pct <= 15 &&
        antiguedad_meses >= 4) {
        return 'Estancados con potencial';
    }

    // 4. Nuevos o sin trazabilidad
    if (antiguedad_meses <= 4) {
        return 'Nuevos o sin trazabilidad';
    }

    // 5. Inestables o en Riesgo (Evaluated last to cover failing cases)
    if (moratoria_promedio_pct >= 25 || participacion_adquirida_pct <= 1) {
        return 'Inestables o en riesgo';
    }

    // Default catch-all
    return 'Estables en construcción';
}

window.calcularEstadoLlamada = function (cliente) {
    const segmentacion_valeria = window.clasificarSegmentacionValeria(cliente);
    const grupo_meta_compra = window.clasificarGrupoMeta(cliente);
    const ultima_llamada = cliente.ultima_llamada;

    // Definir cadencia en meses según segmento
    const segmento = segmentacion_valeria.split(' ')[0];
    let cadenciaMeses;

    if (segmento === 'Avanzados') cadenciaMeses = 2;
    else if (segmento === 'Estables' || segmento === 'Construyendo') cadenciaMeses = 6;
    else if (segmento === 'Inestables' || grupo_meta_compra === 'En riesgo') cadenciaMeses = 4;
    else if (segmento === 'Estancados') cadenciaMeses = 3;
    else if (segmento === 'Nuevos') cadenciaMeses = 1;
    else cadenciaMeses = 6;

    // Calcular días desde última llamada
    if (!ultima_llamada) {
        return { estado: 'Pendiente agendar', color: 'var(--color-danger)', diasRestantes: -Infinity };
    }

    const fechaLlamada = new Date(ultima_llamada);
    const hoy = new Date();
    const diffTime = Math.abs(hoy - fechaLlamada);
    const diasDesde = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    const diasCadencia = cadenciaMeses * 30;
    const diasRestantes = diasCadencia - diasDesde;

    // Determinar estado
    if (diasDesde >= diasCadencia) {
        return { estado: 'Pendiente agendar', color: 'var(--color-danger)', diasRestantes };
    } else if (diasDesde >= diasCadencia * 0.8) {
        return { estado: 'Próxima a agendar', color: 'var(--color-warning)', diasRestantes };
    } else {
        return { estado: 'Al día', color: 'var(--color-success)', diasRestantes };
    }
}

window.strategies = {
    'Avanzados': 'Felicitar por el progreso. Motivar a la compra de equity final para alcanzar el 30%. Explorar si tienen ahorros extras para abonos capital.',
    'Estables': 'Reforzar el hábito de pago. Explicar el beneficio de la proyección a 5 años. Incentivar pequeños aportes adicionales para mejorar el score.',
    'Estancados': 'Diagnóstico profundo sobre por qué no han comprado equity. Ofrecer asesoría financiera básica para liberar DTI y mejorar capacidad de compra.',
    'Inestables': 'Llamada de alerta temprana. Identificar motivos de la mora. Establecer plan de pagos urgente y explicar riesgo de pérdida de vivienda.',
    'Nuevos': 'Llamada de bienvenida y educación. Explicar cómo funciona el equity y el score dentro del modelo Duppla. Resolver dudas iniciales.'
};
