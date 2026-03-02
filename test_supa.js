const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = "https://ygkjtoefbeaxlkludjvp.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inlna2p0b2VmYmVheGxrbHVkanZwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzExNzEyOTYsImV4cCI6MjA4Njc0NzI5Nn0._-f6Gs1n4VhQPm_PVtb_Yuj799IObVo9wTqLZfRQ_ss";

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

async function test() {
    const { data, error } = await supabase.from('druo_no_conectados').select('*');
    if (error) {
        console.error("ERROR:", error);
    } else {
        console.log("DATA LENGTH:", data ? data.length : 0);
        if (data && data.length > 0) {
            console.log("FIRST ROW:", data[0]);
        }
    }
}

test();
