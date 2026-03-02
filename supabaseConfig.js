const SUPABASE_URL = "https://ygkjtoefbeaxlkludjvp.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inlna2p0b2VmYmVheGxrbHVkanZwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzExNzEyOTYsImV4cCI6MjA4Njc0NzI5Nn0._-f6Gs1n4VhQPm_PVtb_Yuj799IObVo9wTqLZfRQ_ss";

const _supabase = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

window.supabaseClient = _supabase;
