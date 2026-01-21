export function PipelineAnimatedArrow() {
  return (
    <div className='relative flex items-center justify-center w-24 h-8'>
      {/* Background glow effect */}
      <div className='absolute inset-0 bg-gradient-to-r from-transparent via-primary/10 to-transparent animate-[shimmer_3s_ease-in-out_infinite]' />

      {/* Particle flow container */}
      <div className='relative flex items-center justify-center w-full h-full'>
        {/* Flowing particles */}
        {[0, 1, 2, 3, 4, 5].map((i) => (
          <div
            key={i}
            className='absolute left-0 h-1.5 w-1.5 rounded-full bg-[#1a6ce7] shadow-[0_0_8px_#1a6ce7]'
            style={{
              animation: `flowParticle 2.5s cubic-bezier(0.4, 0, 0.2, 1) infinite`,
              animationDelay: `${i * 0.4}s`,
              opacity: 0
            }}
          />
        ))}

        {/* Arrow head with pulse */}
        <div className='absolute right-0 flex items-center justify-center'>
          <div className='absolute h-3 w-3 rounded-full bg-primary/20 animate-[ping_2s_cubic-bezier(0,0,0.2,1)_infinite]' />
          <svg
            width="16"
            height="16"
            viewBox="0 0 16 16"
            fill="none"
            className='relative z-10 text-primary'
          >
            <path
              d="M3 8h10m0 0l-4-4m4 4l-4 4"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </div>

        {/* Connection line with gradient */}
        <div className='absolute inset-y-0 left-0 right-4 flex items-center'>
          <div className='h-[2px] w-full bg-gradient-to-r from-primary/20 via-primary/40 to-primary/60' />
        </div>
      </div>


      {/* Custom animations */}
      <style dangerouslySetInnerHTML={{
        __html: `
        @keyframes flowParticle {
          0% {
            left: 0%;
            opacity: 0;
            transform: scale(0.5);
          }
          10% {
            opacity: 1;
            transform: scale(1);
          }
          85% {
            opacity: 1;
            transform: scale(1);
          }
          100% {
            left: calc(100% - 20px);
            opacity: 0;
            transform: scale(0.5);
          }
        }
        
        @keyframes shimmer {
          0%, 100% {
            opacity: 0.3;
            transform: translateX(-100%);
          }
          50% {
            opacity: 0.8;
            transform: translateX(100%);
          }
        }
      `}} />
    </div>
  )
}
